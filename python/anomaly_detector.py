import argparse
import json
import uuid

from event_bus import emit_event
from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def detect_anomalies(days: int = 14) -> int:
    conn = get_conn()
    count = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH series AS (
                  SELECT
                    DATE_TRUNC('HOUR', q.start_time) AS ts_hour,
                    COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
                    COALESCE(q.warehouse_name, 'UNKNOWN') AS warehouse_name,
                    COALESCE(q.user_name, 'UNKNOWN') AS user_name,
                    SUM(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0)) AS credits,
                    SUM(COALESCE(q.total_elapsed_time, 0)) / 1000 AS elapsed_seconds,
                    SUM(COALESCE(q.bytes_scanned, 0)) / POW(1024, 3) AS scanned_gb,
                    SUM(COALESCE(q.bytes_spilled_to_local_storage, 0) + COALESCE(q.bytes_spilled_to_remote_storage, 0)) / POW(1024, 3) AS spill_gb,
                    SUM(COALESCE(q.queued_overload_time, 0) + COALESCE(q.queued_provisioning_time, 0)) / 1000 AS queue_seconds
                  FROM COST_COPILOT.FACT_QUERY q
                  LEFT JOIN COST_COPILOT.FACT_QUERY_COST c
                    ON q.query_id = c.query_id
                  WHERE q.start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                  GROUP BY 1,2,3,4
                ),
                scores AS (
                  SELECT
                    *,
                    AVG(credits) OVER (PARTITION BY query_tag ORDER BY ts_hour ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING) AS credits_baseline,
                    STDDEV_SAMP(credits) OVER (PARTITION BY query_tag ORDER BY ts_hour ROWS BETWEEN 48 PRECEDING AND 1 PRECEDING) AS credits_std
                  FROM series
                )
                SELECT
                  ts_hour, query_tag, warehouse_name, user_name,
                  credits, elapsed_seconds, scanned_gb, spill_gb, queue_seconds,
                  COALESCE(credits_baseline, 0) AS credits_baseline,
                  IFF(COALESCE(credits_std, 0) = 0, 0, (credits - credits_baseline) / NULLIF(credits_std, 0)) AS z_score
                FROM scores
                WHERE credits > 0
                  AND COALESCE(credits_baseline, 0) > 0
                  AND IFF(COALESCE(credits_std, 0) = 0, 0, (credits - credits_baseline) / NULLIF(credits_std, 0)) > 2.5
                ORDER BY z_score DESC
                LIMIT 200
                """,
                {"days": days},
            )
            for row in cur.fetchall():
                (
                    ts_hour,
                    query_tag,
                    warehouse_name,
                    user_name,
                    credits,
                    elapsed_seconds,
                    scanned_gb,
                    spill_gb,
                    queue_seconds,
                    baseline,
                    z_score,
                ) = row
                severity = "CRITICAL" if z_score >= 5 else "HIGH" if z_score >= 3 else "MEDIUM"
                cause = "COST_SPIKE"
                if spill_gb > 1:
                    cause = "SPILL_PRESSURE"
                elif queue_seconds > 60:
                    cause = "CONCURRENCY_CONTENTION"
                evidence = {
                    "ts_hour": str(ts_hour),
                    "query_tag": query_tag,
                    "warehouse_name": warehouse_name,
                    "user_name": user_name,
                    "credits": float(credits or 0),
                    "elapsed_seconds": float(elapsed_seconds or 0),
                    "scanned_gb": float(scanned_gb or 0),
                    "spill_gb": float(spill_gb or 0),
                    "queue_seconds": float(queue_seconds or 0),
                }
                anomaly_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO COST_COPILOT.ANOMALIES (
                      anomaly_id, run_id, metric_name, object_type, object_name, owner_id,
                      observed_value, baseline_value, anomaly_score, severity, suspected_cause, evidence_json
                    )
                    SELECT
                      %(anomaly_id)s, %(run_id)s, 'credits', 'QUERY_TAG', %(object_name)s, 'owner-unowned',
                      %(observed)s, %(baseline)s, %(score)s, %(severity)s, %(cause)s, PARSE_JSON(%(evidence)s)
                    """,
                    {
                        "anomaly_id": anomaly_id,
                        "run_id": str(uuid.uuid4()),
                        "object_name": query_tag,
                        "observed": float(credits or 0),
                        "baseline": float(baseline or 0),
                        "score": float(z_score or 0),
                        "severity": severity,
                        "cause": cause,
                        "evidence": json.dumps(evidence),
                    },
                )
                emit_event(
                    cur,
                    "anomaly.detected",
                    {
                        "anomaly_id": anomaly_id,
                        "query_tag": query_tag,
                        "severity": severity,
                        "anomaly_score": float(z_score or 0),
                    },
                )
                count += 1
            conn.commit()
    finally:
        conn.close()
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    count = detect_anomalies(days=args.days)
    print(f"Detected {count} anomalies.")


if __name__ == "__main__":
    main()
