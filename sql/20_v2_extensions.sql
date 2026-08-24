USE SCHEMA COST_COPILOT;

CREATE TABLE IF NOT EXISTS COST_COPILOT.DIM_ACCOUNT (
  account_name STRING,
  account_locator STRING,
  environment STRING,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.DIM_OWNER (
  owner_id STRING,
  team_name STRING,
  owner_name STRING,
  owner_email STRING,
  slack_channel STRING,
  severity_policy STRING,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.JOB_OWNER_MAP (
  map_id STRING,
  query_tag_pattern STRING,
  job_name STRING,
  owner_id STRING,
  priority NUMBER DEFAULT 100,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.ANOMALIES (
  anomaly_id STRING,
  run_id STRING,
  metric_name STRING,
  object_type STRING,
  object_name STRING,
  owner_id STRING,
  observed_value FLOAT,
  baseline_value FLOAT,
  anomaly_score FLOAT,
  severity STRING,
  suspected_cause STRING,
  evidence_json VARIANT,
  detected_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  status STRING DEFAULT 'OPEN',
  CONSTRAINT PK_ANOMALIES PRIMARY KEY (anomaly_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.BUDGETS (
  budget_id STRING,
  budget_name STRING,
  scope_type STRING,
  scope_value STRING,
  period STRING,
  budget_credits FLOAT,
  budget_usd FLOAT,
  warning_threshold_pct FLOAT DEFAULT 0.8,
  critical_threshold_pct FLOAT DEFAULT 1.0,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_BUDGETS PRIMARY KEY (budget_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.BUDGET_EVENTS (
  event_id STRING,
  budget_id STRING,
  window_start TIMESTAMP_LTZ,
  window_end TIMESTAMP_LTZ,
  observed_credits FLOAT,
  observed_usd FLOAT,
  burn_rate FLOAT,
  days_to_exhaustion FLOAT,
  threshold_level STRING,
  evidence_json VARIANT,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_BUDGET_EVENTS PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.FACT_LINEAGE_EDGES (
  edge_id STRING,
  src_type STRING,
  src_name STRING,
  dst_type STRING,
  dst_name STRING,
  relation_type STRING,
  weight FLOAT,
  evidence_json VARIANT,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_LINEAGE_EDGES PRIMARY KEY (edge_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.SIMULATION_RUNS (
  simulation_id STRING,
  run_name STRING,
  target_type STRING,
  target_name STRING,
  assumptions_json VARIANT,
  created_by STRING,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_SIM_RUNS PRIMARY KEY (simulation_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.SIMULATION_RESULTS (
  result_id STRING,
  simulation_id STRING,
  scenario_name STRING,
  expected_credit_delta FLOAT,
  expected_runtime_delta_pct FLOAT,
  expected_usd_delta FLOAT,
  confidence_score FLOAT,
  assumptions_json VARIANT,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_SIM_RESULTS PRIMARY KEY (result_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.RECOMMENDATION_SCORES (
  score_id STRING,
  rec_id STRING,
  evidence_completeness_score FLOAT,
  consistency_score FLOAT,
  confidence_calibration_score FLOAT,
  final_quality_score FLOAT,
  suppression_reason STRING,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_REC_SCORES PRIMARY KEY (score_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.PLAYBOOKS (
  playbook_id STRING,
  recommendation_type STRING,
  playbook_name STRING,
  prerequisites STRING,
  quick_fix STRING,
  best_fix STRING,
  long_term_fix STRING,
  validation_sql STRING,
  risk_class STRING,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_PLAYBOOKS PRIMARY KEY (playbook_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.PLAYBOOK_EXECUTIONS (
  execution_id STRING,
  playbook_id STRING,
  rec_id STRING,
  execution_mode STRING,
  execution_status STRING,
  notes STRING,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_PLAYBOOK_EXECUTIONS PRIMARY KEY (execution_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.VERIFICATION_RESULTS (
  verification_id STRING,
  rec_id STRING,
  action_id STRING,
  pre_window_start TIMESTAMP_LTZ,
  pre_window_end TIMESTAMP_LTZ,
  post_window_start TIMESTAMP_LTZ,
  post_window_end TIMESTAMP_LTZ,
  metric_name STRING,
  pre_value FLOAT,
  post_value FLOAT,
  change_pct FLOAT,
  verdict STRING,
  evidence_json VARIANT,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_VERIFICATION_RESULTS PRIMARY KEY (verification_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.RECURRENCE_MEMORY (
  memory_id STRING,
  fingerprint STRING,
  object_type STRING,
  object_name STRING,
  first_seen TIMESTAMP_LTZ,
  last_seen TIMESTAMP_LTZ,
  occurrence_count NUMBER,
  last_verdict STRING,
  severity STRING,
  context_json VARIANT,
  CONSTRAINT PK_RECURRENCE_MEMORY PRIMARY KEY (memory_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.SLA_POLICIES (
  sla_id STRING,
  scope_type STRING,
  scope_value STRING,
  metric_name STRING,
  target_value FLOAT,
  comparator STRING,
  policy_action STRING,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_SLA_POLICIES PRIMARY KEY (sla_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.EVENT_STREAM (
  event_id STRING,
  event_type STRING,
  event_payload VARIANT,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_EVENT_STREAM PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.RAG_DOCUMENTS (
  doc_id STRING,
  source_type STRING,
  source_name STRING,
  doc_text STRING,
  metadata_json VARIANT,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_RAG_DOCUMENTS PRIMARY KEY (doc_id)
);

CREATE TABLE IF NOT EXISTS COST_COPILOT.RAG_CHUNKS (
  chunk_id STRING,
  doc_id STRING,
  source_type STRING,
  source_name STRING,
  chunk_text STRING,
  metadata_json VARIANT,
  created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_RAG_CHUNKS PRIMARY KEY (chunk_id)
);

MERGE INTO COST_COPILOT.DIM_OWNER t
USING (
  SELECT 'owner-unowned' AS owner_id, 'UNOWNED' AS team_name, 'Unowned Workload' AS owner_name, NULL AS owner_email, NULL AS slack_channel, 'MEDIUM' AS severity_policy
) s
ON t.owner_id = s.owner_id
WHEN NOT MATCHED THEN INSERT (owner_id, team_name, owner_name, owner_email, slack_channel, severity_policy)
VALUES (s.owner_id, s.team_name, s.owner_name, s.owner_email, s.slack_channel, s.severity_policy);
