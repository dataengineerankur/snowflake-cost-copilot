USE SCHEMA COST_COPILOT;

CREATE OR REPLACE VIEW COST_COPILOT.V_COST_SPIKES AS
SELECT
  object_type,
  object_name,
  metric_name,
  severity,
  anomaly_score,
  observed_value,
  baseline_value,
  detected_at
FROM COST_COPILOT.ANOMALIES
ORDER BY detected_at DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_OWNER_HEATMAP AS
SELECT
  COALESCE(o.team_name, 'UNOWNED') AS team_name,
  COUNT(*) AS recommendation_count,
  ROUND(SUM(COALESCE(r.est_savings, 0)), 2) AS est_savings
FROM COST_COPILOT.RECOMMENDATIONS r
LEFT JOIN COST_COPILOT.DIM_OWNER o
  ON o.owner_id = COALESCE(r.evidence_json:owner_id::STRING, 'owner-unowned')
GROUP BY 1
ORDER BY est_savings DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_RECURRENT_OFFENDERS AS
SELECT
  object_type,
  object_name,
  occurrence_count,
  last_seen,
  severity,
  last_verdict
FROM COST_COPILOT.RECURRENCE_MEMORY
ORDER BY occurrence_count DESC, last_seen DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_ACTION_EFFECTIVENESS AS
SELECT
  v.rec_id,
  v.metric_name,
  v.pre_value,
  v.post_value,
  v.change_pct,
  v.verdict,
  a.action_status,
  a.created_at AS action_time
FROM COST_COPILOT.VERIFICATION_RESULTS v
LEFT JOIN COST_COPILOT.ACTION_LOG a
  ON v.action_id = a.action_id
ORDER BY v.created_at DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_SIMULATION_TOP_OPPORTUNITIES AS
SELECT
  simulation_id,
  scenario_name,
  expected_credit_delta,
  expected_usd_delta,
  confidence_score,
  assumptions_json
FROM COST_COPILOT.SIMULATION_RESULTS
ORDER BY expected_credit_delta ASC, confidence_score DESC;

CREATE OR REPLACE VIEW COST_COPILOT.V_ANOMALY_STREAM AS
SELECT
  anomaly_id AS event_id,
  'anomaly.detected' AS event_type,
  OBJECT_CONSTRUCT(
    'object_type', object_type,
    'object_name', object_name,
    'metric_name', metric_name,
    'severity', severity,
    'score', anomaly_score
  ) AS event_payload,
  detected_at AS created_at
FROM COST_COPILOT.ANOMALIES
UNION ALL
SELECT
  rec_id AS event_id,
  'recommendation.created' AS event_type,
  OBJECT_CONSTRUCT(
    'rule_name', rule_name,
    'recommendation_type', recommendation_type,
    'object_name', object_name,
    'risk', risk
  ),
  created_at
FROM COST_COPILOT.RECOMMENDATIONS;
