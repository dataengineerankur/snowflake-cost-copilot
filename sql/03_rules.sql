USE SCHEMA COST_COPILOT;

-- Optional SQL-native rules. The Python rules engine is authoritative.
-- This file provides direct SQL diagnostics for debugging/tuning thresholds.

-- RULE_HEAVY_JOBS diagnostic
WITH heavy AS (
  SELECT
    COALESCE(q.query_tag, 'UNTAGGED') AS query_tag,
    q.warehouse_name,
    q.user_name,
    ROUND(SUM(COALESCE(c.credits_attributed_compute, 0) + COALESCE(c.credits_used_query_acceleration, 0)), 4) AS credits_total,
    ROUND(SUM(COALESCE(q.total_elapsed_time, 0)) / 1000, 2) AS elapsed_seconds,
    ROUND(SUM(COALESCE(q.bytes_scanned, 0)) / POW(1024, 3), 2) AS scanned_gb,
    ARRAY_AGG(q.query_id) WITHIN GROUP (ORDER BY COALESCE(c.credits_attributed_compute, 0) DESC) AS query_ids
  FROM COST_COPILOT.FACT_QUERY q
  LEFT JOIN COST_COPILOT.FACT_QUERY_COST c
    ON q.query_id = c.query_id
  WHERE q.start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  GROUP BY 1, 2, 3
)
SELECT *
FROM heavy
ORDER BY credits_total DESC
LIMIT 25;

-- RULE_SPILL diagnostic
SELECT
  query_tag,
  warehouse_name,
  ROUND(SUM(spill_gb), 3) AS spill_gb,
  ROUND(SUM(credits_total), 3) AS credits_total
FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS
GROUP BY 1, 2
HAVING SUM(spill_gb) > 5
ORDER BY spill_gb DESC;

-- RULE_IDLE_WAREHOUSE diagnostic
SELECT *
FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS
WHERE credits_used > 5
  AND avg_running < 0.25
ORDER BY credits_used DESC;
