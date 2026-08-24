import argparse
import json
import uuid
from typing import Any, Dict, List

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def _to_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_config(cur) -> Dict[str, Any]:
    cur.execute("SELECT config_key, config_value FROM COST_COPILOT.CONFIG")
    result = {}
    for key, value in cur.fetchall():
        if hasattr(value, "as_dict"):
            value = value.as_dict()
        result[key] = value
    return result


def _insert_rec(
    cur,
    run_id: str,
    rule_name: str,
    object_type: str,
    object_name: str,
    recommendation_type: str,
    risk: str,
    evidence: Dict[str, Any],
    suggested_fix: str,
    ddl_sql: str,
    rollback_sql: str,
    est_savings: float,
):
    rec_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO COST_COPILOT.RECOMMENDATIONS (
          rec_id, run_id, rule_name, object_type, object_name, recommendation_type, risk,
          evidence_json, suggested_fix, ddl_sql, rollback_sql, est_savings
        )
        SELECT
          %(rec_id)s, %(run_id)s, %(rule_name)s, %(object_type)s, %(object_name)s, %(recommendation_type)s, %(risk)s,
          PARSE_JSON(%(evidence)s), %(suggested_fix)s, %(ddl_sql)s, %(rollback_sql)s, %(est_savings)s
        """,
        {
            "rec_id": rec_id,
            "run_id": run_id,
            "rule_name": rule_name,
            "object_type": object_type,
            "object_name": object_name,
            "recommendation_type": recommendation_type,
            "risk": risk,
            "evidence": json.dumps(evidence),
            "suggested_fix": suggested_fix,
            "ddl_sql": ddl_sql,
            "rollback_sql": rollback_sql,
            "est_savings": est_savings,
        },
    )


def _top_query_ids(cur, query_tag: str, warehouse_name: str, user_name: str, days: int, limit: int = 5) -> List[str]:
    cur.execute(
        """
        SELECT q.query_id
        FROM COST_COPILOT.FACT_QUERY q
        LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
        WHERE COALESCE(q.query_tag, 'UNTAGGED') = %(query_tag)s
          AND COALESCE(q.warehouse_name, 'UNKNOWN') = %(warehouse_name)s
          AND COALESCE(q.user_name, 'UNKNOWN') = %(user_name)s
          AND q.start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
        ORDER BY COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0) DESC
        LIMIT %(limit)s
        """,
        {"query_tag": query_tag, "warehouse_name": warehouse_name, "user_name": user_name, "days": days, "limit": limit},
    )
    return [r[0] for r in cur.fetchall()]


def generate_recommendations(days: int = 7) -> int:
    conn = get_conn()
    generated = 0
    run_id = str(uuid.uuid4())
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            config = _load_config(cur)
            spill_gb_threshold = _to_float(config.get("SPILL_GB_THRESHOLD"), 5.0)
            scan_gb_threshold = _to_float(config.get("SCAN_GB_THRESHOLD"), 100.0)
            pruning_threshold = _to_float(config.get("PRUNING_RATIO_THRESHOLD"), 0.8)
            queue_sec_threshold = _to_float(config.get("QUEUE_SECONDS_THRESHOLD"), 60.0)
            compile_threshold = _to_float(config.get("COMPILE_RATIO_THRESHOLD"), 0.25)
            idle_credits_threshold = _to_float(config.get("IDLE_CREDITS_PER_HOUR_THRESHOLD"), 1.5)
            pipe_failure_threshold = _to_float(config.get("PIPE_FAILURE_THRESHOLD"), 5.0)

            cur.execute(
                """
                SELECT
                  COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
                  COALESCE(q.warehouse_name, 'UNKNOWN') AS warehouse_name,
                  COALESCE(q.user_name, 'UNKNOWN') AS user_name,
                  COUNT(*) AS query_count,
                  SUM(COALESCE(q.total_elapsed_time, 0)) / 1000 AS elapsed_seconds,
                  SUM(COALESCE(q.bytes_scanned, 0)) / POW(1024, 3) AS scanned_gb,
                  SUM(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0)) AS credits
                FROM COST_COPILOT.FACT_QUERY q
                LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
                WHERE q.start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                GROUP BY 1,2,3
                ORDER BY credits DESC, elapsed_seconds DESC
                LIMIT 50
                """,
                {"days": days},
            )
            for row in cur.fetchall():
                query_tag, warehouse_name, user_name, query_count, elapsed_seconds, scanned_gb, credits = row
                top_query_ids = _top_query_ids(cur, query_tag, warehouse_name, user_name, days)
                evidence = {
                    "window_days": days,
                    "query_tag": query_tag,
                    "warehouse_name": warehouse_name,
                    "user_name": user_name,
                    "query_count": int(query_count or 0),
                    "elapsed_seconds": _to_float(elapsed_seconds),
                    "scanned_gb": _to_float(scanned_gb),
                    "credits": _to_float(credits),
                    "top_query_ids": top_query_ids,
                }
                _insert_rec(
                    cur,
                    run_id=run_id,
                    rule_name="RULE_HEAVY_JOBS",
                    object_type="JOB",
                    object_name=query_tag,
                    recommendation_type="HEAVY_JOB",
                    risk="MEDIUM",
                    evidence=evidence,
                    suggested_fix="Prioritize top expensive statements for query tuning and pruning improvements.",
                    ddl_sql="",
                    rollback_sql="",
                    est_savings=_to_float(credits) * 0.10,
                )
                generated += 1

            cur.execute(
                """
                SELECT
                  COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
                  COALESCE(q.warehouse_name, 'UNKNOWN') AS warehouse_name,
                  SUM((COALESCE(bytes_spilled_to_local_storage, 0) + COALESCE(bytes_spilled_to_remote_storage, 0)) / POW(1024, 3)) AS spill_gb,
                  SUM(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0)) AS credits,
                  ARRAY_AGG(q.query_id) WITHIN GROUP (ORDER BY COALESCE(c.credits_attributed_compute, 0) DESC) AS query_ids
                FROM COST_COPILOT.FACT_QUERY q
                LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
                WHERE q.start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                GROUP BY 1,2
                HAVING spill_gb > %(spill_gb_threshold)s
                ORDER BY spill_gb DESC
                LIMIT 25
                """,
                {"days": days, "spill_gb_threshold": spill_gb_threshold},
            )
            for query_tag, warehouse_name, spill_gb, credits, query_ids in cur.fetchall():
                evidence = {
                    "window_days": days,
                    "query_tag": query_tag,
                    "warehouse_name": warehouse_name,
                    "spill_gb": _to_float(spill_gb),
                    "credits": _to_float(credits),
                    "top_query_ids": list(query_ids or [])[:10],
                }
                _insert_rec(
                    cur,
                    run_id,
                    "RULE_SPILL",
                    "WAREHOUSE",
                    warehouse_name,
                    "MEMORY_PRESSURE",
                    "MEDIUM",
                    evidence,
                    "Investigate joins and aggregation cardinality; consider right-sizing warehouse memory for this workload.",
                    "",
                    "",
                    _to_float(credits) * 0.15,
                )
                generated += 1

            cur.execute(
                """
                SELECT
                  COALESCE(query_tag, 'UNTAGGED') AS query_tag,
                  COALESCE(warehouse_name, 'UNKNOWN') AS warehouse_name,
                  AVG(IFF(COALESCE(partitions_total, 0) = 0, NULL, COALESCE(partitions_scanned, 0) / NULLIF(partitions_total, 0))) AS pruning_ratio,
                  SUM(COALESCE(bytes_scanned, 0)) / POW(1024, 3) AS scanned_gb,
                  ARRAY_AGG(query_id) AS query_ids
                FROM COST_COPILOT.FACT_QUERY
                WHERE start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                GROUP BY 1,2
                HAVING (pruning_ratio > %(pruning_threshold)s AND pruning_ratio IS NOT NULL) OR scanned_gb > %(scan_gb_threshold)s
                ORDER BY scanned_gb DESC
                LIMIT 25
                """,
                {"days": days, "pruning_threshold": pruning_threshold, "scan_gb_threshold": scan_gb_threshold},
            )
            for query_tag, warehouse_name, pruning_ratio, scanned_gb, query_ids in cur.fetchall():
                evidence = {
                    "window_days": days,
                    "query_tag": query_tag,
                    "warehouse_name": warehouse_name,
                    "pruning_ratio": _to_float(pruning_ratio),
                    "scanned_gb": _to_float(scanned_gb),
                    "top_query_ids": list(query_ids or [])[:10],
                }
                _insert_rec(
                    cur,
                    run_id,
                    "RULE_SCAN_PRUNING",
                    "JOB",
                    query_tag,
                    "BAD_PRUNING",
                    "MEDIUM",
                    evidence,
                    "Improve clustering/filter predicates to reduce full scans and increase partition pruning.",
                    "",
                    "",
                    _to_float(scanned_gb) * 0.01,
                )
                generated += 1

            cur.execute(
                """
                SELECT
                  warehouse_name,
                  SUM(COALESCE(queued_overload_time, 0) + COALESCE(queued_provisioning_time, 0)) / 1000 AS queue_seconds,
                  SUM(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0)) AS credits,
                  ARRAY_AGG(q.query_id) AS query_ids
                FROM COST_COPILOT.FACT_QUERY q
                LEFT JOIN COST_COPILOT.FACT_QUERY_COST c ON q.query_id = c.query_id
                WHERE q.start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                GROUP BY 1
                HAVING queue_seconds > %(queue_sec_threshold)s
                ORDER BY queue_seconds DESC
                LIMIT 20
                """,
                {"days": days, "queue_sec_threshold": queue_sec_threshold},
            )
            for warehouse_name, queue_seconds, credits, query_ids in cur.fetchall():
                evidence = {
                    "window_days": days,
                    "warehouse_name": warehouse_name,
                    "queue_seconds": _to_float(queue_seconds),
                    "credits": _to_float(credits),
                    "top_query_ids": list(query_ids or [])[:10],
                }
                _insert_rec(
                    cur,
                    run_id,
                    "RULE_QUEUE",
                    "WAREHOUSE",
                    warehouse_name,
                    "CONGESTION",
                    "MEDIUM",
                    evidence,
                    "Increase concurrency capacity (warehouse size or max cluster count) and spread schedules.",
                    "",
                    "",
                    _to_float(credits) * 0.05,
                )
                generated += 1

            cur.execute(
                """
                SELECT
                  COALESCE(query_tag, 'UNTAGGED') AS query_tag,
                  AVG(COALESCE(compilation_time, 0) / NULLIF(total_elapsed_time, 0)) AS compile_ratio,
                  COUNT(*) AS query_count,
                  ARRAY_AGG(query_id) AS query_ids
                FROM COST_COPILOT.FACT_QUERY
                WHERE start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                  AND total_elapsed_time > 0
                GROUP BY 1
                HAVING compile_ratio > %(compile_threshold)s
                ORDER BY compile_ratio DESC
                LIMIT 20
                """,
                {"days": days, "compile_threshold": compile_threshold},
            )
            for query_tag, compile_ratio, query_count, query_ids in cur.fetchall():
                evidence = {
                    "window_days": days,
                    "query_tag": query_tag,
                    "compile_ratio": _to_float(compile_ratio),
                    "query_count": int(query_count or 0),
                    "top_query_ids": list(query_ids or [])[:10],
                }
                _insert_rec(
                    cur,
                    run_id,
                    "RULE_COMPILE",
                    "JOB",
                    query_tag,
                    "SQL_COMPLEXITY",
                    "MEDIUM",
                    evidence,
                    "Reduce SQL complexity (flatten nested CTEs, stable SQL templates, materialize repeated subqueries).",
                    "",
                    "",
                    max(1.0, int(query_count or 0) * 0.1),
                )
                generated += 1

            cur.execute(
                """
                SELECT
                  warehouse_name,
                  AVG(COALESCE(avg_running, 0)) AS avg_running,
                  SUM(COALESCE(credits_used, 0)) AS credits_used
                FROM COST_COPILOT.FACT_WAREHOUSE_HOURLY
                WHERE start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                GROUP BY 1
                HAVING SUM(COALESCE(credits_used, 0)) > %(idle_credits_threshold)s * %(days)s
                   AND AVG(COALESCE(avg_running, 0)) < 0.30
                ORDER BY credits_used DESC
                LIMIT 20
                """,
                {"days": days, "idle_credits_threshold": idle_credits_threshold},
            )
            for warehouse_name, avg_running, credits_used in cur.fetchall():
                safe_wh = warehouse_name.replace('"', "")
                ddl = f'ALTER WAREHOUSE "{safe_wh}" SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE'
                rollback = f'ALTER WAREHOUSE "{safe_wh}" SET AUTO_SUSPEND = 300 AUTO_RESUME = TRUE'
                evidence = {
                    "window_days": days,
                    "warehouse_name": warehouse_name,
                    "avg_running": _to_float(avg_running),
                    "credits_used": _to_float(credits_used),
                }
                _insert_rec(
                    cur,
                    run_id,
                    "RULE_IDLE_WAREHOUSE",
                    "WAREHOUSE",
                    warehouse_name,
                    "IDLE_BURN",
                    "LOW",
                    evidence,
                    "Lower idle burn by tightening auto-suspend while preserving auto-resume for interactive use.",
                    ddl,
                    rollback,
                    _to_float(credits_used) * 0.20,
                )
                generated += 1

            cur.execute(
                """
                SELECT
                  pipe_name,
                  SUM(COALESCE(copy_failure_count, 0)) AS failures,
                  SUM(COALESCE(small_file_loads, 0)) AS small_file_loads,
                  SUM(COALESCE(credits_used, 0)) AS credits_used
                FROM COST_COPILOT.FACT_PIPE_DAILY
                WHERE usage_date >= DATEADD(DAY, -%(days)s, CURRENT_DATE())
                GROUP BY 1
                HAVING SUM(COALESCE(copy_failure_count, 0)) > %(pipe_failure_threshold)s
                    OR SUM(COALESCE(small_file_loads, 0)) > 25
                ORDER BY failures DESC, small_file_loads DESC
                LIMIT 20
                """,
                {"days": days, "pipe_failure_threshold": pipe_failure_threshold},
            )
            for pipe_name, failures, small_file_loads, credits_used in cur.fetchall():
                evidence = {
                    "window_days": days,
                    "pipe_name": pipe_name,
                    "copy_failures": int(failures or 0),
                    "small_file_loads": int(small_file_loads or 0),
                    "credits_used": _to_float(credits_used),
                }
                _insert_rec(
                    cur,
                    run_id,
                    "RULE_PIPE_NOISE",
                    "PIPE",
                    pipe_name,
                    "INGESTION_WASTE",
                    "MEDIUM",
                    evidence,
                    "Reduce tiny files with upstream micro-batching and triage recurring load failures from COPY history.",
                    "",
                    "",
                    _to_float(credits_used) * 0.12,
                )
                generated += 1

            conn.commit()
    finally:
        conn.close()
    return generated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    count = generate_recommendations(days=args.days)
    print(f"Generated {count} rule-based recommendations.")


if __name__ == "__main__":
    main()
