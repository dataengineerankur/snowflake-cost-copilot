import argparse
import json
import uuid
from typing import List

import snowflake.connector
from policy_engine import evaluate_policy
from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def _load_allowlist(cur) -> List[str]:
    cur.execute("SELECT config_value FROM COST_COPILOT.CONFIG WHERE config_key = 'APPLY_WAREHOUSE_ALLOWLIST'")
    row = cur.fetchone()
    if not row:
        return []
    if isinstance(row, dict):
        value = row.get("CONFIG_VALUE")
    else:
        value = row[0]
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    if isinstance(value, list):
        return value
    return []


def _is_safe_ddl(ddl_sql: str) -> bool:
    if not ddl_sql:
        return False
    normalized = " ".join(ddl_sql.upper().split())
    if not normalized.startswith("ALTER WAREHOUSE"):
        return False
    return "AUTO_SUSPEND" in normalized or "AUTO_RESUME" in normalized


def apply_actions(mode: str = "DRY_RUN") -> int:
    conn = get_conn()
    applied = 0
    mode = mode.upper()
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            allowlist = list({w.upper() for w in _load_allowlist(cur)})
            cur.execute("SELECT rec_id, approved, approved_by FROM COST_COPILOT.APPROVALS")
            approvals = [{k.lower(): v for k, v in row.items()} for row in cur.fetchall()]
            cur.execute(
                """
                SELECT r.rec_id, r.object_name, r.ddl_sql, r.rollback_sql, r.risk, r.recommendation_type
                FROM COST_COPILOT.RECOMMENDATIONS r
                WHERE r.status = 'OPEN'
                  AND r.ddl_sql IS NOT NULL
                  AND r.ddl_sql <> ''
                ORDER BY r.created_at DESC
                """
            )
            candidates = cur.fetchall()
            for rec in candidates:
                warehouse = (rec["OBJECT_NAME"] or "").upper()
                ddl_sql = rec["DDL_SQL"] or ""
                rollback_sql = rec["ROLLBACK_SQL"] or ""
                action_status = "SKIPPED"
                before_state = {}
                verification_query = f"SHOW WAREHOUSES LIKE '{warehouse}'"

                rec_obj = {k.lower(): v for k, v in rec.items()}
                allowed, reason = evaluate_policy(rec_obj, approvals, allowlist)
                if not allowed:
                    action_status = f"SKIPPED_{reason}"
                elif not _is_safe_ddl(ddl_sql):
                    action_status = "SKIPPED_UNSAFE_DDL"
                else:
                    cur.execute(verification_query)
                    before_state = {"warehouse": warehouse, "show_warehouses_result": cur.fetchall()}
                    if mode == "APPLY":
                        cur.execute(ddl_sql)
                        cur.execute("UPDATE COST_COPILOT.RECOMMENDATIONS SET status = 'APPLIED' WHERE rec_id = %(rec_id)s", {"rec_id": rec["REC_ID"]})
                        action_status = "APPLIED"
                        applied += 1
                    else:
                        action_status = "DRY_RUN"

                cur.execute(
                    """
                    INSERT INTO COST_COPILOT.ACTION_LOG (
                      action_id, rec_id, mode, action_status, ddl_executed, before_state,
                      rollback_sql, verification_query, executor
                    )
                    SELECT
                      %(action_id)s, %(rec_id)s, %(mode)s, %(action_status)s, %(ddl_executed)s, PARSE_JSON(%(before_state)s),
                      %(rollback_sql)s, %(verification_query)s, CURRENT_USER()
                    """,
                    {
                        "action_id": str(uuid.uuid4()),
                        "rec_id": rec["REC_ID"],
                        "mode": mode,
                        "action_status": action_status,
                        "ddl_executed": ddl_sql,
                        "before_state": json.dumps(before_state, default=str),
                        "rollback_sql": rollback_sql,
                        "verification_query": verification_query,
                    },
                )
            conn.commit()
    finally:
        conn.close()
    return applied


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["DRY_RUN", "APPLY"], default="DRY_RUN")
    args = parser.parse_args()
    applied = apply_actions(mode=args.mode)
    print(f"Executor completed in {args.mode}. Applied actions: {applied}")


if __name__ == "__main__":
    main()
