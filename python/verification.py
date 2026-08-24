import argparse
import json
import uuid

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def run_verification(days: int = 3) -> int:
    conn = get_conn()
    created = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.action_id, a.rec_id, r.object_name
                FROM COST_COPILOT.ACTION_LOG a
                JOIN COST_COPILOT.RECOMMENDATIONS r ON r.rec_id = a.rec_id
                WHERE a.action_status IN ('APPLIED', 'DRY_RUN')
                ORDER BY a.created_at DESC
                LIMIT 50
                """
            )
            for action_id, rec_id, object_name in cur.fetchall():
                cur.execute(
                    """
                    SELECT
                      AVG(COALESCE(total_elapsed_time, 0)) / 1000
                    FROM COST_COPILOT.FACT_QUERY
                    WHERE COALESCE(query_tag, 'UNTAGGED') = %(tag)s
                      AND start_time BETWEEN DATEADD(DAY, -%(days)s*2, CURRENT_TIMESTAMP()) AND DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                    """,
                    {"tag": object_name, "days": days},
                )
                pre_val = float(cur.fetchone()[0] or 0)
                cur.execute(
                    """
                    SELECT
                      AVG(COALESCE(total_elapsed_time, 0)) / 1000
                    FROM COST_COPILOT.FACT_QUERY
                    WHERE COALESCE(query_tag, 'UNTAGGED') = %(tag)s
                      AND start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                    """,
                    {"tag": object_name, "days": days},
                )
                post_val = float(cur.fetchone()[0] or 0)
                change_pct = ((post_val - pre_val) / pre_val * 100) if pre_val else 0.0
                verdict = "VERIFIED_EFFECTIVE" if change_pct < -5 else "REGRESSED" if change_pct > 10 else "INCONCLUSIVE"
                cur.execute(
                    """
                    INSERT INTO COST_COPILOT.VERIFICATION_RESULTS (
                      verification_id, rec_id, action_id, pre_window_start, pre_window_end, post_window_start, post_window_end,
                      metric_name, pre_value, post_value, change_pct, verdict, evidence_json
                    )
                    SELECT
                      %(verification_id)s, %(rec_id)s, %(action_id)s,
                      DATEADD(DAY, -%(days)s*2, CURRENT_TIMESTAMP()), DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP()),
                      DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(),
                      'avg_elapsed_seconds', %(pre)s, %(post)s, %(change)s, %(verdict)s,
                      PARSE_JSON(%(evidence)s)
                    """,
                    {
                        "verification_id": str(uuid.uuid4()),
                        "rec_id": rec_id,
                        "action_id": action_id,
                        "days": days,
                        "pre": pre_val,
                        "post": post_val,
                        "change": change_pct,
                        "verdict": verdict,
                        "evidence": json.dumps({"object_name": object_name}),
                    },
                )
                cur.execute("UPDATE COST_COPILOT.RECOMMENDATIONS SET status = %(status)s WHERE rec_id = %(rec_id)s", {"status": verdict, "rec_id": rec_id})
                created += 1
            conn.commit()
    finally:
        conn.close()
    return created


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    args = parser.parse_args()
    count = run_verification(days=args.days)
    print(f"Verification rows created: {count}")


if __name__ == "__main__":
    main()
