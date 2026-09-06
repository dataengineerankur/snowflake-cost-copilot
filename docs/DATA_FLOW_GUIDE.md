# Snowflake Cost Copilot: Complete Data Flow Guide

## Overview

This document explains the 7-layer data architecture that powers the Snowflake Cost Copilot. The architecture is designed for **idempotent ingestion**, **rule-based analysis**, **AI-grounded recommendations**, and **governed execution**.

---

## Layer 1: Data Sources (SNOWFLAKE.ACCOUNT_USAGE)

**Purpose**: Native Snowflake telemetry - the single source of truth for all cost/performance metrics.

### Tables Used

| Source | Purpose | Grain | Latency |
|--------|---------|-------|---------|
| `QUERY_HISTORY` | Query execution metrics (time, bytes, spill) | Per query | 45min-2h |
| `QUERY_ATTRIBUTION_HISTORY` | Credits attributed per query | Per query | 2-4h |
| `WAREHOUSE_METERING_HISTORY` | Credits consumed per warehouse per hour | Per hour | 2-4h |
| `WAREHOUSE_LOAD_HISTORY` | Concurrency/queue/running state per warehouse | Per minute | Real-time |
| `PIPE_USAGE_HISTORY` | Snowpipe credit/file metrics | Per day | 24h |
| `COPY_HISTORY` | Data load operations and failures | Per run | 1-2h |
| `TASK_HISTORY` | Scheduled task execution and failures | Per run | 1-2h |
| `TABLE_STORAGE_METRICS` | Table size, time-travel, failsafe bytes | Per day | 1-2h |

### Key Columns Extracted

**QUERY_HISTORY columns**:
- `query_id`: unique identifier
- `query_tag`: custom metadata for job attribution (e.g., `dbt:prod:model=fact_sales`)
- `warehouse_name`, `user_name`, `role_name`
- `start_time`, `total_elapsed_time`, `execution_time`, `compilation_time`
- `bytes_scanned`, `partitions_scanned`, `partitions_total`
- `bytes_spilled_to_local_storage`, `bytes_spilled_to_remote_storage`
- `queued_overload_time`, `queued_provisioning_time`

**QUERY_ATTRIBUTION_HISTORY columns**:
- `query_id`, `query_tag`, `start_time`
- `credits_attributed_compute`, `credits_used_query_acceleration`

---

## Layer 2: Staging Tables (STG_*)

**Purpose**: Idempotent landing zone. Uses **MERGE (upsert)** pattern to avoid duplicates.

### Pattern: MERGE Upsert

Every staging table uses this SQL pattern:

```sql
MERGE INTO COST_COPILOT.STG_QUERY_HISTORY t
USING (
  SELECT query_id, query_tag, warehouse_name, ..., CURRENT_TIMESTAMP() as load_ts
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
) s
ON t.query_id = s.query_id
WHEN MATCHED THEN UPDATE SET t.* = s.*
WHEN NOT MATCHED THEN INSERT *;
```

### Why MERGE?

- **Idempotent**: Safe to re-run without creating duplicates
- **Atomic**: All-or-nothing; partial failures don't corrupt data
- **Efficient**: Only updates changed rows, minimizes cost
- **Reliable**: Handles late-arriving data and updates

### Staging Tables

| Staging Table | Matched On | Purpose |
|---------------|-----------|---------|
| `STG_QUERY_HISTORY` | `query_id` | Raw query metrics from ACCOUNT_USAGE |
| `STG_QUERY_ATTRIBUTION_HISTORY` | `query_id` | Raw cost data from ACCOUNT_USAGE |
| `STG_WAREHOUSE_METERING_HOURLY` | `warehouse_name, start_time` | Raw warehouse hourly metrics |
| `STG_PIPE_DAILY` | `pipe_name, usage_date` | Raw pipe usage from ACCOUNT_USAGE |
| `STG_TASK_DAILY` | `task_name, usage_date, state` | Raw task history |
| `STG_STORAGE_DAILY` | `table_key, usage_date` | Raw table storage metrics |

---

## Layer 3: Fact Tables

**Purpose**: Clean, normalized facts for analysis. These are the immutable source for all downstream analytics and recommendations.

### Key Design Principles

1. **Grain**: Explicitly defined granularity (one row per query, per warehouse-hour, per pipe-day)
2. **Join-ready**: All foreign keys present; no NULL lookups
3. **Immutable**: Rarely updated; inserts only (or incremental)
4. **Indexed**: Clustered on common filter keys

### Fact Tables

#### FACT_QUERY
- **Grain**: One row per `query_id`
- **Primary Key**: `query_id`
- **Columns**:
  - `query_id`, `query_tag`, `warehouse_name`, `user_name`, `role_name`
  - `start_time`, `end_time`
  - `total_elapsed_time`, `execution_time`, `compilation_time`
  - `bytes_scanned`, `partitions_scanned`, `partitions_total`
  - `bytes_spilled_to_local_storage`, `bytes_spilled_to_remote_storage`
  - `queued_overload_time`, `queued_provisioning_time`
  - `query_text`
- **Used by**: All 7 rules (spill, scan, queue, compile, heavy jobs), anomalies

#### FACT_QUERY_COST
- **Grain**: One row per `query_id` (credit attribution)
- **Primary Key**: `query_id`
- **Columns**:
  - `query_id`, `query_tag`, `start_time`
  - `credits_attributed_compute`, `credits_used_query_acceleration`
- **Used by**: Heavy jobs rule, cost analysis

#### FACT_WAREHOUSE_HOURLY
- **Grain**: One row per warehouse per hour
- **Primary Key**: `warehouse_name, start_time`
- **Columns**:
  - `warehouse_name`, `start_time`, `end_time`
  - `credits_used`, `credits_used_compute`, `credits_used_cloud_services`
  - `avg_running`, `avg_queued_load`, `avg_queued_provisioning`, `avg_blocked`
- **Used by**: Idle burn rule, warehouse health dashboard

#### FACT_PIPE_DAILY
- **Grain**: One row per pipe per day
- **Primary Key**: `pipe_name, usage_date`
- **Columns**:
  - `pipe_name`, `usage_date`
  - `credits_used`, `files_inserted`, `bytes_inserted`, `rows_inserted`
  - `copy_failure_count`, `copy_success_count`, `small_file_loads`
- **Used by**: Ingestion waste rule

#### FACT_TASK_DAILY
- **Grain**: One row per task per day per state
- **Primary Key**: `task_name, usage_date, database_name, schema_name, state`
- **Columns**:
  - `task_name`, `database_name`, `schema_name`, `usage_date`, `state`
  - `run_count`, `failed_count`, `avg_duration_seconds`
- **Used by**: Task health analysis

#### FACT_STORAGE_DAILY
- **Grain**: One row per table per day
- **Primary Key**: `table_catalog, table_schema, table_name, usage_date`
- **Columns**:
  - `table_catalog`, `table_schema`, `table_name`, `usage_date`
  - `active_bytes`, `time_travel_bytes`, `failsafe_bytes`, `retained_for_clone_bytes`
- **Used by**: Storage optimization

---

## Layer 4: Dimension Tables (DIM_*)

**Purpose**: Enrich facts with business context. Enable team/owner attribution and drilldown.

### Dimension Tables

#### DIM_JOB (parsed from QUERY_TAG)
- **Grain**: Unique `query_tag`
- **Columns**: `query_tag`, `job_name`, `dag_id`, `task_id`, `run_id`, `env`, `model_name`
- **Purpose**: Maps query_tag to semantic job identity
- **Example**: From `dbt:prod:model=fact_sales:run=abc123`, extract job_name=`fact_sales`, env=`prod`

#### DIM_WAREHOUSE
- **Grain**: Unique `warehouse_name`
- **Columns**: `warehouse_name`, `warehouse_size`, `warehouse_type`, `is_auto_suspend_enabled`
- **Purpose**: Warehouse configuration for sizing recommendations

#### DIM_USER
- **Grain**: Unique `user_name`
- **Columns**: `user_name`
- **Purpose**: User identity for attribution

#### DIM_ROLE
- **Grain**: Unique `role_name`
- **Columns**: `role_name`
- **Purpose**: Role-based filtering and governance

#### DIM_OWNER
- **Grain**: Unique owner (mapped from DIM_JOB + team mapping)
- **Columns**: `owner_name`, `owner_email`, `team`, `environment`
- **Purpose**: Team accountability; route recommendations to right owner

#### DIM_PIPE
- **Grain**: Unique `pipe_name`
- **Columns**: `pipe_name`, `database_name`, `schema_name`
- **Purpose**: Pipe configuration and ownership

#### DIM_TASK
- **Grain**: Unique `task_name`
- **Columns**: `task_name`, `database_name`, `schema_name`
- **Purpose**: Task configuration and ownership

---

## Layer 5: Analysis and Output Tables

**Purpose**: Recommendations, anomalies, and action logs produced by the rules engine and AI layer.

### RECOMMENDATIONS
- **Grain**: One row per recommendation
- **Columns**:
  - `rec_id`, `run_id`, `rule_name`, `recommendation_type`
  - `object_type` (QUERY, WAREHOUSE, JOB, PIPE, TASK), `object_name`
  - `risk` (LOW, MED, HIGH), `evidence_json`
  - `suggested_fix`, `ddl_sql`, `rollback_sql`
  - `est_savings`, `created_at`, `status` (OPEN, APPLIED, VERIFIED_EFFECTIVE, REGRESSED)
- **Used by**: AI recommender, executor, verification, chat

### AI_RECOMMENDATIONS
- **Grain**: One per recommendation
- **Columns**:
  - `ai_rec_id`, `rec_id`
  - `ai_mode` (openrouter, groq, template)
  - `ai_summary`, `ai_root_cause`
  - `ai_fix_options_json`, `ai_verification_steps`
  - `confidence_score`
- **Purpose**: LLM-grounded explanations and ranked fixes

### ACTION_LOG
- **Grain**: One per executed action
- **Columns**:
  - `action_id`, `rec_id`, `mode` (DRY_RUN, APPLY)
  - `ddl_executed`, `before_state`, `rollback_sql`
  - `executor`, `created_at`
- **Purpose**: Complete audit trail for governance

### ANOMALIES
- **Grain**: One per detected anomaly
- **Columns**:
  - `detected_at`, `severity` (LOW, MED, HIGH)
  - `anomaly_score`, `baseline_value`, `observed_value`
  - `suspected_cause`, `affected_object`
- **Purpose**: Early warning system for cost spikes

---

## Layer 6: Query Views (V_*)

**Purpose**: Materialized or virtual views that answer common business questions. Used by chat, dashboards, and rule definitions.

### Views

| View | Purpose | Query |
|------|---------|-------|
| `V_HEAVY_TAGS_7D` | Top 20 expensive query tags (last 7 days) | Join FACT_QUERY + FACT_QUERY_COST, group by query_tag, order by credits |
| `V_QUERY_WASTE_SIGNALS` | Queries with spill > threshold or scan ratio > threshold | FACT_QUERY where spill_gb > 5 or scan_ratio > 0.9 |
| `V_WAREHOUSE_IDLE_SIGNALS` | Warehouses with low utilization but high cost | FACT_WAREHOUSE_HOURLY where avg_running < 0.5 and credits_used > threshold |
| `V_PIPE_HEALTH_SIGNALS` | Pipes with failures or small-file patterns | FACT_PIPE_DAILY where failure_count > 5 or small_file_loads > threshold |
| `V_TOP_RECOMMENDATIONS` | Recommendations ranked by impact, not already applied | RECOMMENDATIONS where status = OPEN order by est_savings desc |

---

## Layer 7: RAG Indexing and Chat Interface

**Purpose**: Enable semantic retrieval and multi-agent reasoning for intelligent chat responses.

### RAG Tables

#### RAG_DOCUMENTS
- **Grain**: One document per table/view/metadata
- **Columns**: `doc_id`, `source_table`, `document_type`, `content`, `metadata_json`, `created_at`
- **Purpose**: Index all copilot data for semantic search

#### RAG_CHUNKS
- **Grain**: Semantic chunks of documents
- **Columns**: `chunk_id`, `doc_id`, `content`, `embedding`, `chunk_index`
- **Purpose**: Enable vector-based retrieval

### Chat API (Multi-Agent Pipeline)

**Flow**:
1. **Planner**: LLM reads question, decides which data/views to retrieve
2. **Analyst**: Execute retrieval queries, fetch context
3. **Responder**: Generate natural-language answer grounded in retrieved evidence

**Example Flow**:
```
User: "Why did costs spike yesterday?"
  ↓
Planner: "Need ANOMALIES + FACT_QUERY_COST + affected warehouse metrics"
  ↓
Analyst: Execute queries against ANOMALIES, FACT_WAREHOUSE_HOURLY for yesterday
  ↓
Responder: "Warehouse ANALYTICS_XL had 2.5x normal load yesterday due to heavy scans.
           Top query spilled 45GB. Recommendation: tune table pruning."
```

---

## Complete Data Flow: Collector → Rules → Executor → Verification

### 1. **Collector** (runs hourly)
```
For each staging table:
  SELECT from SNOWFLAKE.ACCOUNT_USAGE (last 7 days)
  MERGE INTO STG_* (idempotent upsert)
  
Then:
  INSERT INTO FACT_* (incremental from staging)
  REFRESH DIM_* (slowly changing dimensions)
  REFRESH V_* (materialized views)
  REFRESH RAG_DOCUMENTS + RAG_CHUNKS (semantic index)
```

### 2. **Rules Engine** (runs after collector)
```
For each rule:
  Query FACT_* tables
  Detect anomalies/waste patterns
  INSERT into RECOMMENDATIONS with evidence_json
  
Rules include:
  - HEAVY_JOBS: Top queries by credits + time
  - SPILL: Remote/local spill > threshold
  - PRUNING: Full scan ratio or large unpartitioned scan
  - QUEUE: Warehouse queue time > threshold
  - COMPILE: Compilation ratio > threshold
  - IDLE_BURN: Warehouse credits high, avg_running low
  - PIPE_NOISE: Pipe failures or small-file patterns
```

### 3. **AI Recommender** (runs after rules)
```
For each recommendation (OPEN status):
  Retrieve evidence row + top query_ids
  Fetch supplemental context (warehouse stats, query text)
  Call OpenRouter/Groq with prompt:
    "Based on this evidence, provide summary, root_cause, fix_options, verification_steps"
  Validate LLM output (no hallucinated metrics)
  INSERT into AI_RECOMMENDATIONS with confidence_score
```

### 4. **Executor** (runs on demand or scheduled)
```
For each recommendation:
  Check: is risk=LOW and approved=true and allowlisted?
  If yes:
    Execute ddl_sql in DRY_RUN or APPLY mode
    Store before_state snapshot
    Create rollback plan
    INSERT into ACTION_LOG
    Call verification logic
  If no:
    Keep status=OPEN (recommendation-only)
```

### 5. **Verification** (runs post-apply)
```
For each applied action:
  Compare pre-metrics vs post-metrics (next 1-3 runs)
  Calculate actual_savings
  Mark recommendation as VERIFIED_EFFECTIVE, INEFFECTIVE, or REGRESSED
  Update ACTION_LOG with outcome
```

---

## Key Design Decisions

### Why MERGE for Staging?

**Idempotency**: If collector fails mid-run, re-run without creating duplicates.

**Example**:
```
Run 1: Load 50,000 queries, fail at 40,000
Run 2 (retry): Load same 50,000 again
  Result: No duplicates (MERGE matches on query_id, updates if changed)
```

### Why Separate Facts from Dimensions?

**Facts** are immutable, event-driven (a query either ran or didn't).
**Dimensions** are slowly changing (warehouse config may change once/week).

This separation enables:
- Fast fact inserts (append-only)
- Dimension lookups are cached
- Late-arriving dimension changes don't break historical facts

### Why RAG Over Traditional Search?

**Semantic retrieval** understands intent (`"Why are costs high?"` → retrieve anomalies, not just search for "high").
**Grounding** ensures AI answers cite specific query_ids, tables, metrics (no hallucination).

---

## Troubleshooting

### Issue: Recommendations don't match reality

**Check**:
1. Collector latency (ACCOUNT_USAGE can lag 2-4h)
2. Facts were updated: `SELECT COUNT(*) FROM FACT_QUERY WHERE load_ts > CURRENT_TIMESTAMP() - INTERVAL '1 hour'`
3. Rule thresholds match environment (dev vs prod)

### Issue: AI recommendation is vague

**Check**:
1. Evidence JSON is populated in RECOMMENDATIONS
2. LLM provider key is set and valid
3. Supplemental queries returned results (not empty)

### Issue: PR creation fails

**Check**:
1. `GITHUB_TOKEN` has `repo` and `contents` scope
2. `GITHUB_REPO_URL` is correct and accessible
3. Git is authenticated in shell: `gh auth status`

---

## Next Steps

1. **Run collector**: `python python/collector.py --days 7`
2. **Generate recommendations**: `python python/rules.py`
3. **Ask copilot**: `curl -X POST http://localhost:8000/api/chat -H "Content-Type: application/json" -d '{"question": "worst jobs?"}'`
4. **Review and apply**: Visit Streamlit UI, approve LOW-risk recommendations, create PRs

