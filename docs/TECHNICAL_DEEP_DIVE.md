# Snowflake Cost Copilot: Technical Deep Dive

## Part 1: Pipeline Architecture & Each Script

### Complete Data Flow

```
SNOWFLAKE.ACCOUNT_USAGE
    ↓
[COLLECTOR] → Extracts raw telemetry
    ↓
FACT_QUERY, FACT_QUERY_COST, FACT_WAREHOUSE_HOURLY
    ↓
[RULES ENGINE] → Pattern detection (7 rules)
    ↓
RECOMMENDATIONS (rules-based)
    ↓
[AI RECOMMENDER] → LLM grounding (optional)
    ↓
AI_RECOMMENDATIONS (AI-ranked)
    ↓
[POLICY ENGINE + EXECUTOR] → Apply if approved
    ↓
ACTION_LOG
    ↓
[VERIFICATION] → Measure impact
    ↓
VERIFICATION_RESULTS
```

---

## Part 2: ACCOUNT_USAGE → Facts & Dimensions

### Step 1: Data Ingestion (COLLECTOR)

**File:** `python/collector.py`

The collector pulls raw telemetry from Snowflake's system views into **staging tables**, then merges into **fact tables**.

#### Data Sources (Snowflake System Views)

| Source | Purpose | Frequency |
|--------|---------|-----------|
| `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` | Query execution details (elapsed, spill, scan, queue) | Per query |
| `SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY` | Query→credits attribution (compute + acceleration) | Per query |
| `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` | Warehouse hourly credits consumed | Hourly |
| `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY` | Warehouse queue & provisioning times | Hourly |
| `SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY` | Pipe failures, file counts, bytes | Daily |
| `SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY` | Task run counts, failures, duration | Daily |
| `SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS` | Storage bytes (active, time-travel, clone) | Daily |

#### The Merge Pattern (Upsert)

```python
# 1. Pull last N days into staging
TRUNCATE TABLE COST_COPILOT.STG_QUERY_HISTORY
INSERT INTO COST_COPILOT.STG_QUERY_HISTORY
SELECT query_id, query_tag, warehouse_name, ... 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())

# 2. Merge (upsert) into fact table
MERGE INTO COST_COPILOT.FACT_QUERY t
USING COST_COPILOT.STG_QUERY_HISTORY s
ON t.query_id = s.query_id
WHEN MATCHED THEN UPDATE SET t.query_tag = s.query_tag, ...
WHEN NOT MATCHED THEN INSERT (...)
```

**Why MERGE?**
- Idempotent — run multiple times, same result
- Fast — only updates changed rows
- Handles late-arriving data from ACCOUNT_USAGE (has 24-45hr latency)

### Step 2: Fact Tables Created

**Query Facts** (`FACT_QUERY`)
```
query_id          | Unique query ID
query_tag         | User-assigned tag (e.g., "ETL_PIPELINE")
warehouse_name    | Warehouse used
user_name         | Who ran it
total_elapsed_time| Milliseconds to execute
execution_time    | Pure execution (excludes queue/compile)
compilation_time  | Query plan compilation
bytes_scanned     | Data read from storage
bytes_spilled_*   | Memory pressure indicator
queued_*_time     | Queue delays (resource contention)
query_text        | Full SQL (truncated)
```

**Query Cost Facts** (`FACT_QUERY_COST`)
```
query_id                      | Link to FACT_QUERY
credits_attributed_compute    | Compute credits charged
credits_used_query_acceleration| Q.Acceleration credits
```

**Warehouse Facts** (`FACT_WAREHOUSE_HOURLY`)
```
warehouse_name             | Warehouse ID
start_time / end_time      | 1-hour window
credits_used               | Total credits in hour
credits_used_compute       | Warehouse compute portion
credits_used_cloud_services| Background services (replication, etc)
avg_running                | Average query running %
avg_queued_load            | Queue contention (0-1)
```

**Pipe & Task Facts**
- `FACT_PIPE_DAILY` — copy job stats (failures, small files)
- `FACT_TASK_DAILY` — task runs, failures, duration

### Step 3: Dimension Tables (Optional but Useful)

| Table | Purpose |
|-------|---------|
| `DIM_JOB` | Map query_tag → job metadata (DAG, task, model) |
| `DIM_WAREHOUSE` | Warehouse config (size, auto-suspend) |
| `DIM_USER` | User directory |
| `DIM_ROLE` | Role hierarchy |

These enable richer aggregations (e.g., "cost by team") but are optional; rules work with facts alone.

---

## Part 3: The 7 Rules Engine

**File:** `python/rules.py`

Rules are **deterministic pattern detectors**. Each rule:
1. Queries the fact tables
2. Identifies workload patterns
3. Generates recommendations with evidence

### Rule 1: RULE_HEAVY_JOBS

**What it detects:**
Jobs consuming 10%+ of total spend

**SQL Logic:**
```sql
SELECT
  query_tag, warehouse_name, user_name,
  COUNT(*) as query_count,
  SUM(credits) as total_credits,
  SUM(bytes_scanned) as scanned_gb
FROM FACT_QUERY
GROUP BY 1,2,3
ORDER BY total_credits DESC
LIMIT 50
```

**Threshold:** None — ranks by spend automatically

**Output:**
- Rec ID: `HEAVY_JOB_<tag>_<warehouse>`
- Risk: `MEDIUM`
- Evidence: Top 5 query IDs, total credits, elapsed time
- Est. Savings: `credits * 10%` (typical tuning impact)
- Suggested Fix: "Tune queries; improve pruning"

**Without AI:**
Provides raw top-spender list; helps ops prioritize.

**With AI:**
LLM explains *why* queries are expensive (joins, aggregation cardinality, scan patterns).

---

### Rule 2: RULE_SPILL

**What it detects:**
Memory pressure (data spilled to disk during execution)

**SQL Logic:**
```sql
SELECT
  query_tag, warehouse_name,
  SUM(bytes_spilled_to_local_storage + 
      bytes_spilled_to_remote_storage) / POW(1024,3) as spill_gb,
  SUM(credits) as total_credits
FROM FACT_QUERY
GROUP BY 1,2
HAVING spill_gb > 5.0  -- Threshold (configurable)
```

**Threshold:** `SPILL_GB_THRESHOLD` (default 5.0 GB)

**Root Cause:**
- Joins with large result sets
- Aggregations exceeding warehouse memory
- Suboptimal query plans

**Est. Savings:** `credits * 15%` (warehouse resize or query rewrite)

**Without AI:**
Flag warehouse as undersized; recommend manual investigation.

**With AI:**
"Remote spill at XGB suggests your joins/aggregations exceed memory for X-Large warehouse. Consider Y-Large or denormalization."

---

### Rule 3: RULE_SCAN_PRUNING

**What it detects:**
Full table scans instead of partition-pruned scans

**SQL Logic:**
```sql
SELECT
  query_tag, warehouse_name,
  AVG(partitions_scanned / NULLIF(partitions_total, 0)) as pruning_ratio,
  SUM(bytes_scanned) as scanned_gb,
  COUNT(*) as query_count
FROM FACT_QUERY
GROUP BY 1,2
HAVING pruning_ratio > 0.8  -- 80%+ of partitions scanned = weak pruning
   OR scanned_gb > 100      -- AND large scan volume
```

**Threshold:**
- `PRUNING_RATIO_THRESHOLD` (default 0.8 = 80%)
- `SCAN_GB_THRESHOLD` (default 100 GB)

**Root Cause:**
- Missing WHERE clauses on partition key
- Dynamic SQL with non-constant predicates
- Missing clustering keys

**Est. Savings:** `credits * 20%` (typical for good pruning)

**Without AI:**
List tables with weak pruning; suggest review query WHERE clauses.

**With AI:**
"Analysis of your top 3 queries shows partition key predicates missing in 40% of WHERE clauses. Add date filters to reduce scan from 500GB to 50GB."

---

### Rule 4: RULE_QUEUE

**What it detects:**
Queue contention (queries waiting for warehouse slots)

**SQL Logic:**
```sql
SELECT
  warehouse_name,
  SUM(queued_overload_time + queued_provisioning_time) / 1000 as queue_seconds,
  COUNT(*) as query_count,
  SUM(credits) as credits
FROM FACT_QUERY
GROUP BY 1
HAVING queue_seconds > 60  -- 60 seconds total queued
```

**Threshold:** `QUEUE_SECONDS_THRESHOLD` (default 60 sec)

**Root Cause:**
- Too many concurrent queries for warehouse size
- Warehouse auto-suspend/resume causing provisioning delays
- Shared warehouse with competing workloads

**Est. Savings:** `credits * 10%` (parallelism gains)

**Without AI:**
Flag warehouse; suggest increasing concurrency level or splitting workload.

**With AI:**
"Queue delays averaged 2.5 min/query. Your X-Large warehouse is at 85% utilization. Route non-critical jobs to separate warehouse or schedule sequentially."

---

### Rule 5: RULE_COMPILE

**What it detects:**
High compilation overhead (complexity or dynamic SQL)

**SQL Logic:**
```sql
SELECT
  query_tag, warehouse_name,
  AVG(compilation_time / NULLIF(total_elapsed_time, 0)) as compile_ratio,
  SUM(credits) as credits
FROM FACT_QUERY
GROUP BY 1,2
HAVING compile_ratio > 0.25  -- Compilation > 25% of total time
```

**Threshold:** `COMPILE_RATIO_THRESHOLD` (default 0.25 = 25%)

**Root Cause:**
- Heavy use of dynamic SQL (EXECUTE IMMEDIATE)
- Complex CTEs regenerated per query
- Unoptimized views with cascading logic

**Est. Savings:** `credits * 8%`

**Without AI:**
Flag for query review; suggest caching compiled plans.

**With AI:**
"Your nightly pipeline compiles the same 50-line CTE 200 times. Create a materialized view instead; compile once, reuse 200x."

---

### Rule 6: RULE_IDLE_WAREHOUSE

**What it detects:**
Warehouses consuming credits despite low activity

**SQL Logic:**
```sql
SELECT
  warehouse_name,
  SUM(credits_used) as total_credits,
  AVG(avg_running) as avg_running_pct,
  AVG(avg_queued_load) as avg_queue_pct
FROM FACT_WAREHOUSE_HOURLY
GROUP BY 1
HAVING avg_running_pct < 0.3  -- <30% utilization
  AND total_credits > 100      -- But still using significant credits
```

**Threshold:** `IDLE_CREDITS_PER_HOUR_THRESHOLD` (default 1.5 credits/hour)

**Root Cause:**
- Warehouse left running during off-hours
- Auto-suspend disabled
- Oversized for actual workload

**Est. Savings:** `credits * 30%` (enable auto-suspend + right-size)

**Without AI:**
List idle warehouses; suggest disabling or resizing.

**With AI:**
"Your DEV_WH consumes 60 credits/day but only runs 2 hours/day (weekend idling). Enable auto-suspend (10 min) saves ~70% or downsize from X-Large to Medium."

---

### Rule 7: RULE_PIPE_NOISE

**What it detects:**
Inefficient data ingestion (failures, small files)

**SQL Logic:**
```sql
SELECT
  pipe_name, usage_date,
  copy_failure_count,
  small_file_loads,  -- <16 MB
  credits_used
FROM FACT_PIPE_DAILY
WHERE copy_failure_count > 5  -- OR
   OR small_file_loads > 100  -- Many tiny files
```

**Threshold:** Configurable

**Root Cause:**
- Upstream system sending tiny files
- Retries increasing with each failure
- Inefficient copy commands

**Est. Savings:** `credits * 12%` (fewer retries + batching)

**Without AI:**
Flag pipes with failures; suggest retry logic or file batching.

**With AI:**
"Your ingest failures occur when upstream sends <1KB files. Implement 5-minute batching upstream; reduces 500 COPY attempts/day to 288."

---

## Summary: All 7 Rules

| Rule | Detects | Without AI | With AI | Est. Savings |
|------|---------|-----------|---------|--------------|
| **HEAVY_JOBS** | Top spenders | List by spend | Explain why expensive | 10% |
| **SPILL** | Memory pressure | Flag warehouse | Suggest resize/rewrite | 15% |
| **PRUNING** | Weak scans | List tables | Show missing WHERE clauses | 20% |
| **QUEUE** | Contention | List warehouse | Suggest concurrency/schedule | 10% |
| **COMPILE** | Dynamic SQL | Flag for review | Suggest materialization | 8% |
| **IDLE** | Wasted capacity | Suggest disable/resize | Right-size with metrics | 30% |
| **PIPE_NOISE** | Ingestion waste | Flag failures | Suggest batching | 12% |

---

## Part 4: AI Layer (Optional)

**File:** `python/ai_recommender.py`

### How AI Works

#### Step 1: Template Fallback (Always Available)

Even without external AI, templated responses are generated:

```python
def _template_output(rec: Dict, evidence: Dict) -> Tuple[str, str, List[Dict]]:
    # Deterministic template based on recommendation type
    if rec["RECOMMENDATION_TYPE"] == "MEMORY_PRESSURE":
        summary = "Remote spill suggests warehouse undersizing"
        root = f"Spill reached {spill_gb} GB..."
        fixes = [
            {"option": "quick_win", "action": "..."},
            {"option": "best_fix", "action": "..."},
        ]
    return summary, root, fixes
```

**No API call needed** — templates work offline.

#### Step 2: External LLM (Optional)

If `OPENROUTER_API_KEY` is configured, AI **enhances** the template:

```python
def _call_external_llm(prompt: str) -> str:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return None  # Fall back to template
    
    payload = {
        "model": "openai/gpt-5.2",
        "messages": [
            {"role": "system", "content": "You are a Snowflake cost expert. Use only supplied evidence."},
            {"role": "user", "content": prompt}
        ]
    }
    # POST to OpenRouter, return AI response
```

**Prompt Structure:**
```
You are a Snowflake FinOps expert. 

RECOMMENDATION: {rec_type}
OBJECT: {object_name}
EVIDENCE:
- Total credits: {credits}
- Spill volume: {spill_gb} GB
- Query IDs: {top_query_ids}

Provide:
1. Root cause analysis (why this happened)
2. Ranked fix options (quick_win, best_fix, long_term)
3. Verification strategy

NEVER invent numbers. Only use supplied evidence.
```

**LLM Response:**
```json
{
  "summary": "Warehouse XL is undersized for your join cardinality",
  "root_cause": "Your 3 largest queries (QID1, QID2, QID3) each join 5M-row fact tables. Local memory (96GB) insufficient.",
  "fixes": [
    {"option": "quick_win", "action": "Add partition filters to WHERE clause..."},
    {"option": "best_fix", "action": "Upgrade to 2XL warehouse..."},
    {"option": "long_term", "action": "Denormalize fact tables..."}
  ],
  "confidence": 0.87
}
```

### Without AI: Deterministic Mode

If no API key or API is unreachable:

```python
if not api_key or api_unreachable:
    summary, root, fixes, verification, confidence = _template_output(rec, evidence)
    return {
        "ai_mode": "TEMPLATE",
        "ai_summary": summary,  # Templated
        "confidence": 0.65,     # Lower than LLM
        "fixes": fixes
    }
```

**Still useful:**
- ✅ Identifies issue and evidence
- ✅ Suggests 3 fix options
- ✅ Provides verification steps
- ❌ Less contextual/specific

### With AI: Enhanced Mode

```python
if api_key and api_reachable:
    llm_response = _call_external_llm(prompt)
    return {
        "ai_mode": "LLM_GROUNDED",
        "ai_summary": llm_response["summary"],  # AI-written
        "confidence": 0.87,                     # Higher
        "fixes": llm_response["fixes"]
    }
```

**Extra value:**
- ✅ Contextual explanations
- ✅ Nuanced fix prioritization
- ✅ Ownership/team identification
- ✅ Cost-benefit trade-offs explained

---

## Execution Modes: DRY_RUN vs APPLY

### Mode 1: DRY_RUN (Safe Default)

```bash
python python/run_copilot.py --mode DRY_RUN
```

**What happens:**
1. Collector runs → fact tables updated
2. Rules run → recommendations generated
3. AI runs (if enabled) → enhanced explanations
4. **Executor queues up SQL but doesn't execute**

**Output:**
```
RECOMMENDATION: RULE_IDLE
  DDL SQL: ALTER WAREHOUSE DEV_WH SET AUTO_SUSPEND = 10;
  Rollback SQL: ALTER WAREHOUSE DEV_WH SET AUTO_SUSPEND = NULL;
  Est. Savings: 50 credits/month
  Status: QUEUED (not applied)
```

**Use for:**
- Testing
- Review before applying
- Validation

### Mode 2: APPLY

```bash
python python/run_copilot.py --mode APPLY
```

**What happens:**
1. All steps above
2. **Executor checks policy**
3. **If approved + low-risk + on allowlist: executes DDL**
4. Logs action + rollback path

**Policy Checks:**
```python
if (risk == "LOW" and 
    approved == True and 
    target_on_allowlist == True and 
    is_safe_ddl_pattern(ddl)):
    execute_sql(ddl)
    log_action(rec_id, ddl, rollback_sql)
```

**Use for:**
- Production automation
- Nightly remediation jobs

---

## Complete Pipeline Sequence

```
$ python python/run_copilot.py --days 7 --mode DRY_RUN --ai on --verify on

[1] COLLECTOR (Ingest)
    → Query ACCOUNT_USAGE for last 7 days
    → Merge into FACT_QUERY, FACT_QUERY_COST, FACT_WAREHOUSE_HOURLY
    Status: 50,000 queries merged

[2] RULES ENGINE (Detect)
    → RULE_HEAVY_JOBS: Found 20 candidates
    → RULE_SPILL: Found 5 candidates
    → RULE_PRUNING: Found 12 candidates
    → RULE_QUEUE: Found 3 candidates
    → RULE_COMPILE: Found 2 candidates
    → RULE_IDLE: Found 1 candidate
    → RULE_PIPE_NOISE: Found 0 candidates
    Status: 43 recommendations generated

[3] OWNER MAPPING (Enrichment)
    → Map query_tags to teams/owners
    Status: 38 owners identified

[4] LINEAGE BUILDER (Graph)
    → Build query→table→warehouse edges
    Status: 2,500 edges created

[5] ANOMALY DETECTOR (Trends)
    → Compare 7-day window vs rolling 30-day baseline
    → Flag 3 cost spikes, 2 perf regressions
    Status: 5 anomalies detected

[6] BUDGET GUARDRAILS (Thresholds)
    → Check monthly spend vs budget
    Status: Account at 87% of budget (warning)

[7] AI RECOMMENDER (Enhancement)
    → For each of 43 recommendations, call OpenRouter
    → Get contextual explanations + fix rankings
    Status: 43 AI recommendations generated (23 templates, 20 LLM-enhanced)

[8] EXECUTOR (Apply or Queue)
    → Policy checks on each recommendation
    → DRY_RUN: Log SQL without executing
    Status: 0 actions applied (DRY_RUN mode)

[9] RECOMMENDATION SCORER (Prioritization)
    → Score by impact, urgency, complexity
    Status: Recommendations ranked

[10] RECURRENCE (Memory)
    → Update chronic offender list
    → Track which recommendations worked last time
    Status: 8 rules matched recurring patterns

[11] VERIFICATION (Measurement)
    → Query pre/post metrics for any applied actions
    Status: 0 verification records (no APPLY mode)

OUTPUT:
Top 10 heavy QUERY_TAGs
Top 10 rules-based recommendations
Top 10 AI-enhanced recommendations
Top 10 anomalies
Top 10 rule SQL snippets
```

---

## Without AI: How It Still Works

### The Fallback Stack

```
NO OPENROUTER_API_KEY
    ↓
RULES GENERATE RECOMMENDATIONS (facts + evidence)
    ↓
TEMPLATE FALLBACK (deterministic)
    ↓
AI_RECOMMENDATIONS table filled with:
    - ai_mode: "TEMPLATE"
    - ai_summary: "Warehouse undersized for workload"
    - fixes: [
        {"option": "quick_win", "action": "..."},
        {"option": "best_fix", "action": "..."},
      ]
    - confidence: 0.65
    ↓
CHAT API STILL WORKS (uses template responses)
    ↓
UI RENDERS (shows evidence + templated explanations)
```

### Example: RULE_SPILL Without AI

**Rule Output:**
```json
{
  "rec_id": "spill_DEV_WH_20260305",
  "rule_name": "RULE_SPILL",
  "object_name": "DEV_WH",
  "evidence_json": {
    "spill_gb": 47.5,
    "credits": 250.0,
    "top_query_ids": ["abc123", "def456"]
  }
}
```

**Template Output:**
```json
{
  "ai_rec_id": "air_spill_20260305",
  "ai_mode": "TEMPLATE",
  "ai_summary": "Remote/local spill reached 47.5 GB, which suggests joins/aggregations exceed memory for current warehouse sizing.",
  "confidence": 0.65
}
```

**UI Display:**
```
🔴 MEMORY PRESSURE DETECTED
   Warehouse: DEV_WH
   Spill: 47.5 GB
   Credits Impacted: 250.0
   
   Why: Joins/aggregations exceed memory for current warehouse
   
   Quick Wins:
   - Review WHERE clauses in top 2 queries (abc123, def456)
   - Add filter predicates to reduce input cardinality
   
   Best Fix:
   - Upgrade to 2XL warehouse (test first)
   - OR denormalize fact tables
   
   Confidence: 65%
   Cost Impact: 15% potential reduction
```

### Example: RULE_SPILL With AI

**Same Rule Output** (facts unchanged)

**AI Enhancement:**
```python
prompt = """
RECOMMENDATION: MEMORY_PRESSURE
WAREHOUSE: DEV_WH
EVIDENCE:
- Spill: 47.5 GB (local + remote)
- Credits: 250.0 (impacted by spill)
- Top 2 queries: abc123, def456

Provide root cause analysis and ranked fixes.
"""

llm_response = {
  "summary": "Your DEV_WH is undersized for multi-table join cardinality",
  "root_cause": "Queries abc123 and def456 perform 5-table joins with result sets exceeding 96GB warehouse memory. Remote spill to disk increased query time 3.2x.",
  "fixes": [
    {
      "option": "quick_win",
      "action": "Add INNER JOIN filter: WHERE order_date >= DATEADD(DAY, -1, TODAY()) to reduce fact table rows from 500M to 5M. Estimated: 90% spill reduction."
    },
    {
      "option": "best_fix",
      "action": "Upgrade DEV_WH to 2XL (costs +$0.50/hour, saves $40/month in spill overhead). Pre-test with single table upgrade."
    }
  ],
  "confidence": 0.92
}
```

**UI Display:**
```
🔴 MEMORY PRESSURE DETECTED (HIGH CONFIDENCE: 92%)
   Warehouse: DEV_WH
   Spill: 47.5 GB (caused 3.2x slowdown)
   Credits Impacted: 250.0
   
   Root Cause:
   Your 5-table joins in queries abc123 and def456 create intermediate result sets exceeding 96GB 
   warehouse memory. Data spills to disk, increasing query time 3.2x and doubling credit cost.
   
   Quick Win (Estimated 90% spill reduction):
   Add WHERE order_date >= DATEADD(DAY, -1, TODAY()) to fact filters
   Reduces input rows from 500M to 5M before join operations
   
   Best Fix (Estimated $40/month savings):
   Upgrade to 2XL warehouse
   Costs $0.50/hour more but eliminates spill overhead
   Test with single query upgrade first
   
   Confidence: 92%
   Cost Savings: 15% of current spend
```

---

## Comparison Table: With vs Without AI

| Aspect | Without AI (Template) | With AI (LLM) |
|--------|----------------------|--------------|
| **Detection** | ✅ All 7 rules work | ✅ Same |
| **Explanations** | Generic, templated | Contextual, specific |
| **Fix Ranking** | Guideline-based | Evidence-ranked |
| **Cost Estimates** | Historical averages | Contextual to workload |
| **Owner Identification** | Limited (from tags) | Can infer from patterns |
| **Verification Steps** | Generic | Customized |
| **Confidence Scores** | 0.65 (conservative) | 0.75-0.95 (LLM-assessed) |
| **Speed** | <1 min (no API call) | 2-5 min (API calls) |
| **Cost** | Free | ~$0.10 per rec (OpenRouter) |
| **Offline Mode** | ✅ Works | ❌ Requires API |

---

## Example: Full Request Without AI

```bash
$ python python/run_copilot.py --days 7 --mode DRY_RUN --ai off
```

**Output (rules + templates only):**
```
Collector done. Source freshness:
  - QUERY_HISTORY: 2m ago
  - WAREHOUSE_METERING_HISTORY: 1h ago

Rules generated 43 recommendations.

Owner mappings refreshed: 38
Lineage edges: 2,500

Anomalies detected: 5, budget events: 1

AI recommender skipped (--ai off).

Executor completed. Applied actions: 0 (DRY_RUN mode)

Recommendation scores written: 43
Recurrence updates: 8

Verification records written: 0

Top 10 heavy QUERY_TAGs:
- tag=etl_daily | warehouse=PROD_WH | credits=1250.50
- tag=bi_reports | warehouse=BI_WH | credits=840.20
...

Top 10 recommendations (rules):
- REC_001 | RULE_HEAVY_JOBS | HEAVY_JOB | etl_daily | risk=MEDIUM | est_savings=125.00
- REC_002 | RULE_SPILL | MEMORY_PRESSURE | PROD_WH | risk=MEDIUM | est_savings=126.00
...

Top 10 anomalies:
- anomaly_1 | etl_daily | HIGH | score=8.7 | observed=1250.5 | baseline=850.0
...
```

System works entirely offline. Recommendations are actionable without AI.

---

## Summary

| Component | Key Points |
|-----------|-----------|
| **Collector** | Pulls ACCOUNT_USAGE every run, merges into facts (upsert pattern) |
| **Facts** | `FACT_QUERY` (execution), `FACT_QUERY_COST` (credits), `FACT_WAREHOUSE_HOURLY` (capacity) |
| **7 Rules** | Heavy jobs, spill, pruning, queue, compile, idle, pipe_noise—deterministic detection |
| **AI Layer** | Optional LLM enhancement; templates work offline; confidence scores reflect availability |
| **Execution** | DRY_RUN (queue only) or APPLY (with policy checks) |
| **Without AI** | ✅ Works offline, ✅ Rules detect patterns, ✅ Templates provide guidance, ❌ Less context |
| **With AI** | ✅ Richer explanations, ✅ Better prioritization, ✅ Custom fix strategies, ❌ Requires API |
