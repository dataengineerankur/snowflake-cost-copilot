USE SCHEMA COST_COPILOT;

CREATE OR REPLACE VIEW COST_COPILOT.V_HEAVY_TAGS_7D AS
SELECT
  COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
  COALESCE(q.warehouse_name, 'UNKNOWN') AS warehouse_name,
  COALESCE(q.user_name, 'UNKNOWN') AS user_name,
  COUNT(*) AS query_count,
  ROUND(SUM(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0)), 4) AS credits_total,
  ROUND(SUM(COALESCE(q.total_elapsed_time, 0)) / 1000, 2) AS elapsed_seconds,
  ROUND(SUM(COALESCE(q.bytes_scanned, 0)) / POW(1024, 3), 2) AS scanned_gb,
  MIN(q.start_time) AS first_seen,
  MAX(q.start_time) AS last_seen
FROM COST_COPILOT.FACT_QUERY q
LEFT JOIN COST_COPILOT.FACT_QUERY_COST c
  ON q.query_id = c.query_id
WHERE q.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY credits_total DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_QUERY_WASTE_SIGNALS AS
SELECT
  q.query_id,
  COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
  q.warehouse_name,
  q.user_name,
  q.start_time,
  ROUND((COALESCE(q.bytes_spilled_to_local_storage, 0) + COALESCE(q.bytes_spilled_to_remote_storage, 0)) / POW(1024, 3), 3) AS spill_gb,
  ROUND(COALESCE(q.bytes_scanned, 0) / POW(1024, 3), 3) AS scanned_gb,
  IFF(COALESCE(q.partitions_total, 0) = 0, NULL, COALESCE(q.partitions_scanned, 0) / NULLIF(q.partitions_total, 0)) AS pruning_ratio,
  ROUND(COALESCE(q.compilation_time, 0) / NULLIF(q.total_elapsed_time, 0), 3) AS compile_ratio,
  ROUND((COALESCE(q.queued_overload_time, 0) + COALESCE(q.queued_provisioning_time, 0)) / 1000, 2) AS queue_seconds,
  ROUND(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0), 4) AS credits_total
FROM COST_COPILOT.FACT_QUERY q
LEFT JOIN COST_COPILOT.FACT_QUERY_COST c
  ON q.query_id = c.query_id
WHERE q.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP());

CREATE OR REPLACE VIEW COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS AS
SELECT
  warehouse_name,
  DATE_TRUNC('DAY', start_time) AS usage_day,
  ROUND(SUM(COALESCE(credits_used, 0)), 4) AS credits_used,
  ROUND(AVG(COALESCE(avg_running, 0)), 4) AS avg_running,
  ROUND(AVG(COALESCE(avg_queued_load, 0) + COALESCE(avg_queued_provisioning, 0)), 4) AS avg_queued
FROM COST_COPILOT.FACT_WAREHOUSE_HOURLY
WHERE start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY credits_used DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_PIPE_HEALTH_SIGNALS AS
SELECT
  usage_date,
  pipe_name,
  ROUND(SUM(COALESCE(credits_used, 0)), 4) AS credits_used,
  SUM(COALESCE(copy_failure_count, 0)) AS failure_count,
  SUM(COALESCE(copy_success_count, 0)) AS success_count,
  SUM(COALESCE(small_file_loads, 0)) AS small_file_loads,
  ROUND(SUM(COALESCE(bytes_inserted, 0)) / POW(1024, 3), 3) AS bytes_inserted_gb
FROM COST_COPILOT.FACT_PIPE_DAILY
WHERE usage_date >= DATEADD(DAY, -14, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY failure_count DESC, credits_used DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_TOP_RECOMMENDATIONS AS
SELECT
  r.rec_id,
  r.created_at,
  r.rule_name,
  r.object_type,
  r.object_name,
  r.recommendation_type,
  r.risk,
  r.est_savings,
  r.suggested_fix,
  r.ddl_sql,
  r.evidence_json,
  a.ai_mode,
  a.ai_summary,
  a.ai_root_cause,
  a.confidence_score
FROM COST_COPILOT.RECOMMENDATIONS r
LEFT JOIN COST_COPILOT.AI_RECOMMENDATIONS a
  ON r.rec_id = a.rec_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.rec_id ORDER BY a.created_at DESC NULLS LAST) = 1
ORDER BY r.created_at DESC;
