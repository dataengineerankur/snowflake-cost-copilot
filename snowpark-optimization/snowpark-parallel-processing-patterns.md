# Building Spark-Style Parallelism Inside Snowpark

## 1. Executive Summary

The simplest, most practical way to make independent pieces of work run concurrently in Snowpark today is: a **single shared, thread-safe `snowflake.snowpark.Session`** (Snowpark Python ≥1.24.0, server ≥8.46, with the session parameter `PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION` enabled) driving a plain **`concurrent.futures.ThreadPoolExecutor`**, where each worker thread calls `.collect()` or `.collect_nowait()` on an independent DataFrame/query. This is officially documented and supported by Snowflake itself, not a workaround ([Working with DataFrames](https://docs.snowflake.com/en/developer-guide/snowpark/python/working-with-dataframes)). It works because the workload is I/O-bound — each thread blocks on a network round trip to Snowflake, the GIL is released during that wait, and Snowflake's connector gives each thread its own cursor so requests don't corrupt each other's state ([server_connection.py#L210-L216](https://github.com/snowflakedb/snowpark-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L210-L216)). Crucially, **Snowpark has no Spark-equivalent partition/task/executor model to reproduce on the client** — Snowflake already parallelizes a single query across a warehouse's nodes automatically via its own MPP engine, and the Snowpark-Connect-for-Spark compatibility layer explicitly treats Spark's manual parallelism knobs (`spark.executor.memory`, `spark.sql.shuffle.partitions`) as no-ops because "Snowflake manages all compute resources, memory allocation, and parallelism internally" ([Optimizing Snowpark Connect for Spark workloads](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-optimization)). Explicit client-side fan-out is therefore a tool for a narrower job than Spark's: genuinely independent units of work that are not themselves expressible as one well-formed SQL statement — per-entity Python calls, independent model-training runs, or a query whose single-shot optimizer plan is demonstrably suboptimal for the data's shape (as in a documented dbt case study where manual geographic partitioning cut a 24-hour geospatial join to roughly 12 hours at similar credit cost — a genuine, if narrow, win) ([dbt Discourse](https://discourse.getdbt.com/t/breaking-a-large-query-into-parallelized-partitioned-queries/4481)). Applied to an already-well-optimized single aggregation or join on a fixed-size warehouse, fan-out mostly reslices a warehouse's fixed thread pool rather than adding capacity, and one detailed practitioner benchmark found that **94.2% of concurrent-query slowdown came from resource contention, not queuing** ([datageek.blog](https://datageek.blog/2026/04/21/performance-impacts-of-concurrency/)). No live benchmark against a real Snowflake account was run for this research (no account access was available in this session); Section 10 gives the real published numbers that do exist and a ready-to-run harness for the reader's own account.

### Key questions answered

Confidence labels follow the source notes' own scheme: **CONFIRMED** (verified against primary source/code), **LIKELY** (secondary source or reasonable inference from confirmed facts, not independently re-verified), **EXPERIMENTAL** (documented but caveated, edge-case, or not fully specified), **PROPOSED** (this report's own design, not an existing feature).

1. **Can multiple Snowpark DataFrame actions execute concurrently?** Yes, via threads on a thread-safe `Session`, or via `collect_nowait()`/`AsyncJob`. — CONFIRMED
2. **Is `snowpark.Session` thread-safe?** Yes, since Snowpark Python 1.24.0 with server ≥8.46, gated behind `PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION`. — CONFIRMED
3. **Is the underlying Snowflake Python connector thread-safe?** Not generally by itself; Snowpark builds thread safety on top of it using thread-local cursors and locks — the raw connector has no general thread-safety contract. — CONFIRMED
4. **Can multiple threads safely call the same Snowpark Session?** Yes for independent read/write queries, but not for concurrent transactions or concurrent session-configuration changes (`USE DATABASE`, etc.). — CONFIRMED
5. **Is one Session per worker better?** Not as a default — Snowflake's own guidance favors one shared thread-safe session; per-worker sessions are the documented fallback only when threads need isolated transactions or configuration. — CONFIRMED (guidance), LIKELY (as a general "better" claim — no throughput benchmark compares the two head-to-head)
6. **Can Snowpark asynchronously submit multiple queries?** Yes, via `collect_nowait()`, returning an `AsyncJob` per query. — CONFIRMED
7. **What exactly does `AsyncJob` do?** Wraps a query ID plus a dedicated cursor; `.is_done()`/`.status()` poll cheaply, `.result()` polls until completion then fetches rows via `result_scan`. — CONFIRMED (traced in source)
8. **Does async submission mean actual simultaneous query execution?** Not by itself — it only avoids blocking on execution time; simultaneous execution still depends on warehouse concurrency capacity. — CONFIRMED
9. **Can multiple queries run simultaneously on one warehouse?** Yes, up to `MAX_CONCURRENCY_LEVEL` per cluster (default 8); beyond that they queue. — CONFIRMED
10. **What happens when warehouse concurrency limits are reached?** Additional queries queue (`QUEUED_OVERLOAD_TIME`) rather than failing, and/or a multi-cluster warehouse spins up another cluster. — CONFIRMED
11. **Would a multi-cluster warehouse help?** Yes, for many independent queries/sessions — it adds whole additional clusters — but it does not split one single query or one session's serial query stream across clusters. — CONFIRMED
12. **Is there an equivalent to Spark partitions in Snowpark?** No native, general one — the closest is a vectorized UDTF's `PARTITION BY`-defined partition, or a manually written HASH/MOD SQL bucket. — CONFIRMED (no general equivalent)
13. **Can Snowflake micro-partitions be used as logical worker partitions?** No — micro-partitions are an internal storage/pruning unit, not exposed to Snowpark code as a schedulable unit. — CONFIRMED (no source found permitting this)
14. **Can Snowpark expose physical Snowflake partitions?** No — no API in the source or docs exposes physical micro-partition boundaries to client code. — CONFIRMED (absence)
15. **Is HASH-based partition fan-out the best approach?** It's the best-documented general-purpose one (no domain knowledge needed, balances skew), but it costs a per-row hash computation and has no published benchmark proving it beats range/domain partitioning. — LIKELY
16. **Are vectorized UDFs effectively distributed Python workers?** Yes for the UDTF `end_partition`+`PARTITION BY` variant; plain vectorized scalar UDFs batch rows for efficiency but without partition-key control. — CONFIRMED
17. **Are Python UDFs executed across multiple Snowflake compute nodes?** Yes — Snowflake's own engineering blog describes round-robin row redistribution across Python interpreter processes on multiple warehouse nodes. — CONFIRMED
18. **Can Python stored procedures launch child jobs concurrently?** Yes, via `collect_nowait()`/`AsyncJob` inside the handler. — CONFIRMED
19. **Can threading be used inside Snowflake Python stored procedures?** Implied yes (via `joblib`'s `threading` backend), but no explicit "threading is allowed" statement was found; direct primary confirmation is missing. — LIKELY
20. **Can multiprocessing be used?** No — the built-in `multiprocessing` module is explicitly documented as unsupported in Python stored procedures/UDFs; `joblib.Parallel` is the sanctioned substitute. — CONFIRMED
21. **Can Snowflake Scripting ASYNC be combined with Snowpark?** Not directly in the same statement — `ASYNC`/`AWAIT` is SQL-Scripting syntax (`LANGUAGE SQL`), while Snowpark's async is `collect_nowait()`/`AsyncJob` (`LANGUAGE PYTHON`); both submit to the same execution layer but are separate language surfaces. — CONFIRMED
22. **Could a temporary table/work queue coordinate workers?** Architecturally plausible using plain tables plus row-claiming `UPDATE`/`MERGE`, but no documented or published implementation of this pattern was found. — EXPERIMENTAL
23. **Could Snowflake Streams/Tasks implement a worker queue?** Streams are designed for one-stream-per-consumer CDC fan-out, not a shared SKIP LOCKED-style competing-consumers queue; no such pattern is documented. — EXPERIMENTAL (unproven)
24. **Could Container Services implement real Python worker pools?** Yes — SPCS supports arbitrary containers and Snowflake's own Ray-based batch-inference architecture and `EXECUTE JOB SERVICE ... REPLICAS=N` both demonstrate real multi-worker parallelism. — CONFIRMED
25. **Which solution is closest architecturally to Apache Spark?** Snowpark Container Services (a coordinator + N worker containers, optionally with Ray) is the closest — genuine, addressable, long-lived distributed workers. — CONFIRMED (architectural comparison, not benchmarked)
26. **Which solution is simplest for developers?** `ThreadPoolExecutor` + one shared thread-safe `Session` — a handful of lines, no new infrastructure. — CONFIRMED
27. **Which solution has lowest overhead?** Native single-query execution (no client fan-out at all) has the lowest overhead when the workload is SQL-shaped; among fan-out mechanisms, `collect_nowait()`/`AsyncJob` avoids holding a thread blocked but still pays one HTTP round trip per submission. — LIKELY
28. **Which solution scales best?** Snowpark Container Services (bounded only by compute-pool node count) scales furthest; warehouse-bounded mechanisms (threads, Tasks, UDTFs) are capped by `MAX_CONCURRENCY_LEVEL` and warehouse node count. — LIKELY
29. **Which solution costs least?** Letting Snowflake's native query engine parallelize a single well-formed SQL statement costs least for SQL-shaped work — client fan-out on a fixed warehouse adds compilation overhead and contention for no added capacity, per the datageek.blog finding. — CONFIRMED (for the contention/compilation finding), LIKELY (as a general cost ranking)
30. **Which solution should actually be implemented in production?** For most teams: native single-query SQL first; `ThreadPoolExecutor` + shared thread-safe Session for genuinely independent units of work; vectorized UDTFs for per-entity CPU-bound Python; SPCS only when workloads need arbitrary runtimes, GPUs, or a true persistent worker pool. — PROPOSED (this report's synthesis)

## 2. How Spark Parallelism Works

Spark's parallelism model rests on a strict separation between a single coordinating **Driver** process and long-lived **Executor** processes: the driver "must listen for and accept incoming connections from its executors throughout its lifetime," while executors "run computations and store data for your application" and "stay up for the duration of the whole application" ([Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)). The driver never touches bulk data — it builds a logical plan, and its `DAGScheduler` walks that plan backward from an action, cutting it into **stages** at every wide (shuffle) dependency, since narrow-dependency operations like `map`/`filter` "allow pipelined execution" within a stage while shuffle dependencies require "intermediate materialization and network transfer" between stages ([DAGScheduler internals](https://books.japila.pl/apache-spark-internals/scheduler/DAGScheduler/)). The unit that ties storage and scheduling together is the **partition**: "Spark will run one task for each partition of the cluster" ([RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)), and within a stage there is a strict 1:1 mapping between tasks and partitions — the DAGScheduler "creates tasks for every missing partition." Partition count is set at creation (`parallelize(data, n)`, or one partition per input file block) and can be changed explicitly: `repartition(n)` always triggers a full shuffle and can move the count up or down, while `coalesce(n)` only decreases it, avoiding a full shuffle where possible. Since Spark 3.2, **Adaptive Query Execution** (default on) can coalesce `spark.sql.shuffle.partitions` (default 200) down at runtime based on actual statistics, turning what was a purely static, programmer-set knob into a runtime-tunable one.

`mapPartitions` is the construct most relevant to reproducing Snowpark-side parallelism: unlike `map`, which transforms one element at a time, `mapPartitions` hands an entire partition to a function of type `Iterator<T> => Iterator<U>`, letting user code run expensive setup once per partition and control its own batching — the direct ancestor of the pandas-based `mapInPandas` (`Iterator[pd.DataFrame] -> Iterator[pd.DataFrame]`), which lets "expensive initialization ... be performed once per batch" ([Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)). All of this is possible only because Spark transformations are **lazy** — `map`, `filter`, `select` merely record intent, and only an action (`collect`, `count`, `reduce`) triggers computation, which is what lets the DAGScheduler see the whole chain and build a globally-optimized stage graph before running anything ([RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)). On the Python side specifically, PySpark uses Py4J only for driver-side JVM control, not bulk data; actual row-at-a-time Python UDFs run in separate **worker subprocesses** with data pickled/cloudpickled back and forth (slow, because of per-row (de)serialization), while pandas UDFs and `mapInPandas` instead move data as **Apache Arrow** columnar batches directly between JVM and Python, reportedly up to ~100x faster for suitable workloads ([PySpark Internals wiki](https://cwiki.apache.org/confluence/display/SPARK/PySpark+Internals/); [Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)). The mental model to carry forward: **N partitions ⇒ N tasks ⇒ up to N units of concurrent work**, bounded by total executor cores — parallelism in Spark is entirely a function of a knob (partition count) the programmer explicitly controls.

## 3. How Snowpark Executes Work

Snowpark's default `DataFrame.collect()` is architecturally an ordinary, synchronous function-call stack with no thread, coroutine, or background execution anywhere in it — the calling Python thread blocks on a socket read for the query's entire duration. The traced path: `DataFrame.collect(block=True)` calls `self._session._conn.execute(self._plan, block=block, ...)` on the `ServerConnection` ([dataframe.py#L769-L935](https://github.com/snowflakedb/snowpark-python/blob/main/src/snowflake/snowpark/dataframe.py#L769-L935)); `ServerConnection.execute()` calls `run_query(block=True)`, which calls `results_cursor = self._cursor.execute(query, **kwargs)` ([server_connection.py#L511-L549](https://github.com/snowflakedb/snowpark-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L511-L549)). Notably, `self._cursor` is a `@property` that lazily creates and caches a `SnowflakeCursor` **per calling thread** ([server_connection.py#L210-L216](https://github.com/snowflakedb/snowflake-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L210-L216)) — the load-bearing detail behind Session thread safety (Section 12). In the connector, `SnowflakeCursor.execute()` dispatches to `self._execute_helper(...)`, which calls `self._connection.cmd_query(query, ...)` ([cursor.py#L616-L760](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/cursor.py#L616-L760)); `SnowflakeConnection.cmd_query()` builds a JSON payload and issues one synchronous POST via `self.rest.request("/queries/v1/query-request?...")` ([connection.py#L2023-L2087](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/connection.py#L2023-L2087)), which flows through `SessionManager.request()` → `use_session()` → a pooled `requests.Session` — a genuine blocking network call ([session_manager.py#L529-L546](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/session_manager.py#L529-L546)). For a normal synchronous query the method waits for the server to fully execute and return results before returning control; `run_query(block=True)` then converts the buffered cursor result into `Row` objects (or pandas/Arrow) via `self._to_data_or_iter(...)` ([server_connection.py#L556-L637](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L556-L637)).

`collect_nowait()` follows the identical call chain with `block=False`: `run_query(block=False)` calls `execute_async_and_notify_query_listener()`, which calls `self._cursor.execute_async(query, ...)` ([server_connection.py#L478-L498](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L478-L498)) — itself a thin wrapper (`kwargs["_exec_async"] = True; return self.execute(*args, **kwargs)`, [cursor.py#L1158-L1165](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/cursor.py#L1158-L1165)). Setting `_exec_async` sets `_no_results = True`, and `cmd_query()` marks the **same** REST endpoint's JSON body with `"asyncExec": _no_results` — async-ness is a flag on one endpoint, not a separate API surface ([connection.py#L2042-L2046](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/connection.py#L2042-L2046)). When `_no_results` is set, `execute()` skips building a `ResultSet` and returns immediately once Snowflake *accepts* the query (not once it *finishes*), per an explicit source comment: *"execute_async / _no_results returns before building a ResultSet; callers fetch rows later via query_result / get_results_from_sfqid"* ([cursor.py#L1108-L1116](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/cursor.py#L1108-L1116)). Back in Snowpark, this wraps into `AsyncJob(results_cursor["queryId"], query, async_job_plan.session, ...)` ([server_connection.py#L566-L577](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L566-L577)), which stores only the query ID, a `Session` reference, and creates its own dedicated cursor (`self._cursor = session._conn._conn.cursor()`) ([async_job.py#L184-L209](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/async_job.py)). `.is_done()`/`.status()` poll cheaply via `get_query_status(query_id)`; `.result()` calls `get_results_from_sfqid()`, which polls in a `while True` loop (0.5s × backoff, "same wait as JDBC") until the query finishes, then runs `select * from table(result_scan('{sfqid}'))` to pull rows ([cursor.py#L1844-L1900](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/cursor.py#L1844-L1900)). The practical upshot: `collect_nowait()` is "fire-and-get-a-handle," not "fire-and-forget" — the calling thread still pays one full request/response round trip before it returns, just not the query's full execution latency.

## 4. What Snowpark Already Parallelizes Automatically

Snowflake's engine already distributes a single query's execution across every node of its assigned warehouse cluster automatically — this is the entire point of a cloud MPP query engine, and it is the reason a single `session.table(...).groupBy(...).agg(...)` call, with zero explicit parallelism code, already runs as a distributed computation without the developer ever specifying a partition count. This automatic parallelism is real and is exactly what Snowflake's own compatibility statement makes explicit: Snowpark Connect for Spark treats Spark's manual tuning knobs (`spark.executor.memory`, `spark.sql.shuffle.partitions`, etc.) as **no-ops**, because "Snowflake manages all compute resources, memory allocation, and parallelism internally" ([Optimizing Snowpark Connect for Spark workloads](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-optimization)) — a direct, first-party statement that Snowflake's design philosophy is engine-managed parallelism, not user-directed partitioning, for anything expressible as SQL.

What Snowflake's engine does **not** give the developer is any of the following, all of which Spark exposes as first-class controls: there is no API to set or read a partition count for a DataFrame's underlying execution (no `.repartition(n)`/`.coalesce(n)` equivalent that maps to physical execution units); Snowflake's internal **micro-partitions** are a storage/pruning concept for query optimization, not a task-scheduling unit ever exposed to Snowpark code — no source found permits reading or targeting them as logical work units; there is no client-visible task/stage graph, no locality-aware scheduler API, and no `mapPartitionsWithIndex`-style "which partition am I" signal available anywhere in the Python UDF/UDTF surface (confirmed absent by direct review of the vectorized UDTF docs). Warehouse-level automatic scaling (resizing a warehouse, or adding clusters to a multi-cluster warehouse) is the platform's sanctioned lever for "more parallelism," and it operates at a much coarser grain than Spark's per-partition task scheduling: resizing changes node count for a whole warehouse, and multi-cluster scaling adds whole additional clusters for concurrent *queries/sessions* — Snowflake's own documentation does not describe any mechanism for splitting one single query or one session's serial query stream across multiple clusters simultaneously ([Multi-cluster warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster)). In short: Snowflake automates the *within-one-query* distribution problem completely and deliberately hides its mechanics; it gives the developer no dial for controlling *how* that distribution happens, only levers for how much total compute is available (warehouse size, cluster count, `MAX_CONCURRENCY_LEVEL`).

## 5. Available Explicit Parallelism Mechanisms

Each mechanism below is a genuinely different way of creating independent units of work and running them concurrently; none of them give Snowpark a general, Spark-equivalent partition/task control plane, but each is real and, where noted, officially documented.

### 5.1 `ThreadPoolExecutor` with a shared thread-safe Session — CONFIRMED, requires Snowpark Python ≥1.24.0 and server ≥8.46

```python
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from snowflake.snowpark import Session

connection_parameters = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
    # REQUIRED for genuine thread-safe Session behavior; defaults to False.
    "session_parameters": {"PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION": True},
}

session = Session.builder.configs(connection_parameters).create()

def run_partition(partition_id: int, num_partitions: int):
    df = session.sql(
        "SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), %(n)s) = %(p)s"
    ).bind({"n": num_partitions, "p": partition_id}) if False else session.sql(
        f"SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), {num_partitions}) = {partition_id}"
    )
    return partition_id, df.collect()

NUM_PARTITIONS = 8
with ThreadPoolExecutor(max_workers=NUM_PARTITIONS) as pool:
    futures = {
        pool.submit(run_partition, p, NUM_PARTITIONS): p
        for p in range(NUM_PARTITIONS)
    }
    for future in as_completed(futures):
        partition_id, rows = future.result()
        print(f"partition {partition_id}: {len(rows)} rows")
```

**Caveats (from the docstring and developer guide, verbatim in spirit):** a single session does not support concurrent *transactions*; do not mutate session configuration (`USE DATABASE`/`USE SCHEMA`/`USE WAREHOUSE`/`USE ROLE`) from one thread while others are active — `cmd_query()` mutates `_database`/`_schema`/`_warehouse`/`_role` with **no lock** ([connection.py#L2091-L2102](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/connection.py#L2091-L2102)); and a single `AsyncJob` instance's `.result()` must not be called from more than one thread concurrently, since it mutates cursor state (`_result`, `_rownumber`, `_prefetch_hook`) with no lock ([session.py#L5150-L5155](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/session.py#L5150-L5155)).

### 5.2 `multiprocessing`/`ProcessPoolExecutor` and `joblib` — CONFIRMED caveats, EXPERIMENTAL as a client-side pattern

Snowflake explicitly recommends **against** the built-in `multiprocessing` module inside Python stored procedures/UDFs and recommends `joblib.Parallel` instead ([Python stored procedure limitations](https://docs.snowflake.com/en/developer-guide/stored-procedure/python/procedure-python-limitations)). `joblib.Parallel`'s default backend differs by warehouse type — `threading` on a standard warehouse, `loky` (multiprocessing-based) on a Snowpark-optimized warehouse (LIKELY; secondary-sourced, not independently re-verified against primary text). This guidance is documented specifically for **server-side, CPU-bound** work inside one stored-procedure handler (e.g. `joblib.Parallel(n_jobs=-1)(joblib.delayed(sqrt)(i**2) for i in range(10))`), not for client-side fan-out of many Snowpark sessions. No published write-up of "`ProcessPoolExecutor` + one Snowpark Session per worker process" was found in this research; it is architecturally plausible (each process must build its own `Session` after starting — a live `Session`/connection is not picklable and must never be constructed in a parent and passed to children) but unbenchmarked and, since the workload is I/O-bound rather than CPU-bound, offers no known GIL-related advantage over threads while adding process-startup and N-separate-authentication overhead:

```python
# One real, documented Snowflake bug that illustrates the pickling hazard:
# create_connection defined as a local function inside DataFrameReader.dbapi
# was incompatible with multiprocessing until fixed in Snowpark Python 1.40.0.
# Lesson: never construct a Session/connection-producing closure in a parent
# process and hand it to worker processes — each process must build its own.

import multiprocessing as mp
from snowflake.snowpark import Session

def worker(partition_id: int, num_partitions: int, conn_params: dict):
    session = Session.builder.configs(conn_params).create()  # built INSIDE the child
    try:
        df = session.sql(
            f"SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), {num_partitions}) = {partition_id}"
        )
        return partition_id, len(df.collect())
    finally:
        session.close()

if __name__ == "__main__":
    conn_params = {...}  # picklable plain dict, not a Session object
    with mp.Pool(processes=8) as pool:
        results = pool.starmap(worker, [(p, 8, conn_params) for p in range(8)])
```

### 5.3 Multiple Sessions (one per worker) — CONFIRMED as a documented fallback, not the default

Snowflake's guidance positions one-session-per-thread as the fallback specifically for isolated transactions or distinct per-thread configuration, not the default scaling pattern: *"If you need to manage multiple transactions concurrently, it's important to use multiple session objects because multiple threads of a single session do not support concurrent transactions"* ([Working with DataFrames](https://docs.snowflake.com/en/developer-guide/snowpark/python/working-with-dataframes)).

```python
def make_session(conn_params):
    return Session.builder.configs(conn_params).create()

sessions = [make_session(conn_params) for _ in range(NUM_PARTITIONS)]

def run_with_own_session(session, partition_id, num_partitions):
    return session.sql(
        f"SELECT COUNT(*) FROM sales WHERE MOD(ABS(HASH(customer_id)), {num_partitions}) = {partition_id}"
    ).collect()

with ThreadPoolExecutor(max_workers=NUM_PARTITIONS) as pool:
    futures = [
        pool.submit(run_with_own_session, sessions[p], p, NUM_PARTITIONS)
        for p in range(NUM_PARTITIONS)
    ]
    results = [f.result() for f in futures]

for s in sessions:
    s.close()
```

### 5.4 HASH/MOD SQL partition fan-out — CONFIRMED as a technique, LIKELY as "best" without a controlled benchmark

```sql
-- Each of N workers runs one of these, independently and concurrently.
SELECT *
FROM sales
WHERE MOD(ABS(HASH(customer_id)), 8) = 3;   -- worker 3 of 8
```

Documented rationale: modulo hashing "ensures uniform distribution even if the key's values are skewed" and "requires no setup or pre-analysis of the table," but "`HASH()` is computed on-the-fly by Snowflake for every row, which can add a performance cost on very large tables" ([Snowflake Data Extraction: Partitioning and Pagination Strategies](https://kuangbyte.medium.com/snowflake-data-extraction-partitioning-and-pagination-strategies-29400889628d) — secondary-sourced, article not independently fetched in full). No source ties partition boundaries to Snowflake's micro-partition clustering keys explicitly — using a range/date partition aligned to a clustering key is a plausible cheaper alternative but is inference, not a documented recommendation.

### 5.5 `collect_nowait()` / `AsyncJob` — CONFIRMED

```python
jobs = []
for p in range(NUM_PARTITIONS):
    df = session.sql(
        f"SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), {NUM_PARTITIONS}) = {p}"
    )
    jobs.append((p, df.collect_nowait()))   # returns immediately once ACCEPTED, not finished

results = {}
for partition_id, async_job in jobs:
    results[partition_id] = async_job.result()   # blocks until this specific job finishes
```

One real benchmark exists for this exact pattern: 6 independent operations (3 regional loads + 3 segment updates), submitted within <200ms of each other, ran in ~9 minutes sequentially versus ~2 minutes in parallel (bounded by the single longest query) ([Parallel Jobs with Snowpark — Cloudyard](https://cloudyard.in/2025/10/parallel-jobs-with-snowpark-collect_nowait-async_job/)).

### 5.6 Snowflake Scripting `ASYNC`/`AWAIT` — CONFIRMED

```sql
CREATE OR REPLACE PROCEDURE fan_out_inserts()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
  ASYNC (INSERT INTO staging_a SELECT * FROM source WHERE region = 'US');
  ASYNC (INSERT INTO staging_b SELECT * FROM source WHERE region = 'EU');
  ASYNC (INSERT INTO staging_c SELECT * FROM source WHERE region = 'APAC');
  AWAIT ALL;
  RETURN 'Done (Async)';
END;
```

Up to **4,000 asynchronous child jobs** can run concurrently; exceeding this errors. Fire-and-forget is explicitly unsupported: if the procedure/block finishes while a child job is still running, that job is auto-canceled ([Working with asynchronous child jobs](https://docs.snowflake.com/en/developer-guide/snowflake-scripting/asynchronous-child-jobs); [AWAIT reference](https://docs.snowflake.com/en/sql-reference/snowflake-scripting/await)). `ASYNC`/`AWAIT` is SQL-Scripting syntax only (`LANGUAGE SQL`) — it cannot be called from a Python handler, which instead uses `collect_nowait()`/`AsyncJob` for the same purpose. Avoid `RESULT_SCAN(LAST_QUERY_ID())` for joining async results — `LAST_QUERY_ID()` is non-deterministic when multiple async jobs finish close together; use named `RESULTSET` variables instead.

### 5.7 Task DAGs — CONFIRMED, coarse-grained only

```sql
CREATE TASK root_task WAREHOUSE = my_wh SCHEDULE = '60 MINUTE' AS
  CALL prepare_partitions();

CREATE TASK child_task_1 WAREHOUSE = my_wh AFTER root_task AS
  CALL process_partition(1);
CREATE TASK child_task_2 WAREHOUSE = my_wh AFTER root_task AS
  CALL process_partition(2);
-- ... up to a documented ~100 child tasks per DAG (LIKELY; secondary-sourced)

CREATE TASK join_task WAREHOUSE = my_wh AFTER child_task_1, child_task_2 AS
  CALL merge_results();
```

Task-to-task transitions carry seconds-scale overhead (serverless cold starts commonly 5–10s, occasionally 15–30s under load; LIKELY, secondary-sourced), so Task DAGs suit **coarse-grained** orchestration (independent sub-pipelines taking minutes) rather than fine-grained fan-out of many small units.

### 5.8 Vectorized UDFs/UDTFs — the real `mapPartitions` equivalent — CONFIRMED

```python
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.types import PandasDataFrameType, StringType, FloatType
import pandas as pd

@udtf(
    output_schema=PandasDataFrameType([StringType(), FloatType()], ["customer_id", "score"]),
    input_types=[StringType(), FloatType()],
    packages=["pandas"],
)
class ScorePartition:
    def end_partition(self, df: pd.DataFrame) -> pd.DataFrame:
        # df is one ENTIRE logical partition, delivered as a pandas DataFrame —
        # this is Snowpark's actual analog of Spark's mapPartitions(func).
        df["score"] = df["value"].rolling(3).mean().fillna(0.0)
        return df[["customer_id", "score"]]

session.udtf.register(ScorePartition, name="score_partition", is_permanent=False)
```

```sql
SELECT * FROM TABLE(
  score_partition(customer_id, value) OVER (PARTITION BY region)
);
```

A plain vectorized scalar UDF (`@udf(vectorized=True)`) also receives rows as pandas DataFrame batches, but the batch is an engine-chosen chunk (documented as "up to a few thousand rows," soft-capped by `max_batch_size`), **not** a stable, key-defined partition — it looks like `mapPartitions` but lacks its determinism ([Vectorized Python UDFs](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch); [Vectorized Python UDTFs](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-vectorized)). Underneath both, Snowflake's engineering blog confirms real multi-node, multi-process distribution: rows are "redistribute[d] across all Python interpreter processes in different virtual warehouse nodes using a round-robin approach, ensuring full parallelism" ([Parallel Python UDF Optimization](https://www.snowflake.com/en/blog/engineering/snowpark-parallel-python-udf-optimization/)).

### 5.9 Snowpark Container Services — CONFIRMED

```sql
CREATE COMPUTE POOL my_pool
  MIN_NODES = 2 MAX_NODES = 8 INSTANCE_FAMILY = CPU_X64_XS;

EXECUTE JOB SERVICE
  IN COMPUTE POOL my_pool
  NAME = partition_job
  REPLICAS = 8
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: worker
        image: /my_db/my_schema/my_repo/worker:latest
        env:
          NUM_PARTITIONS: "8"
  $$;
```

```python
# Inside the container (worker.py) — connects using the auto-injected,
# auto-refreshing OAuth token, no embedded credentials.
import os
from snowflake.snowpark import Session

partition_id = int(os.environ["SNOWFLAKE_SERVICE_REPLICA_INDEX"])  # illustrative
num_partitions = int(os.environ["NUM_PARTITIONS"])

session = Session.builder.configs({
    "host": os.environ["SNOWFLAKE_HOST"],
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "token": open("/snowflake/session/token").read(),
    "authenticator": "oauth",
}).create()

df = session.sql(
    f"SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), {num_partitions}) = {partition_id}"
)
df.write.save_as_table(f"results_partition_{partition_id}", mode="overwrite")
```

`EXECUTE JOB SERVICE ... REPLICAS=N` natively supports "1 job, N replicas, each replica processes a shard" — Snowflake's own docs give exactly this example: "you might use 10 replicas to process a 10-million-row dataset, with each handling 1 million rows" ([Working with services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-services)). Snowflake's own batch-inference architecture goes further, using a Ray head/worker topology on SPCS where "workers independently read staged Parquet data, perform inference, and write results to designated output stages" ([Snowflake Batch Inference at Scale with SPCS and Ray](https://www.snowflake.com/en/blog/engineering/snowflake-batch-inference-jobs-spcs/)).

## 6. Best Architecture

The architecture that reproduces Spark's ergonomics most faithfully, without requiring capabilities Snowpark doesn't have, is a thin **client-side scheduler over a thread pool over a shared session**, with the actual partition-defining logic pushed into SQL predicates (or, for CPU-bound per-row work, into a vectorized UDTF). It looks like this:

```
                         ┌─────────────────────────────┐
                         │   Driver process (your app)  │
                         │                               │
                         │  1. plan_partitions()         │
                         │     -> [ (id=0, predicate),   │
                         │          (id=1, predicate),   │
                         │          ... ]                │
                         └───────────────┬───────────────┘
                                         │
                         ┌───────────────▼───────────────┐
                         │  Shared thread-safe Session     │
                         │  (PYTHON_SNOWPARK_ENABLE_       │
                         │   THREAD_SAFE_SESSION=True)     │
                         └───────────────┬───────────────┘
                                         │
        ┌────────────────┬──────────────┼──────────────┬────────────────┐
        ▼                ▼               ▼              ▼                ▼
  ThreadPoolExecutor worker threads (one thread-local cursor each)
        │                │               │              │                │
        ▼                ▼               ▼              ▼                ▼
   session.sql(pred_0)  pred_1        pred_2         pred_3           pred_N
   .collect_nowait()    .collect_nowait() ...
        │                │               │              │                │
        └────────────────┴──────┬────────┴──────────────┴────────────────┘
                                 ▼
                    Snowflake warehouse (MAX_CONCURRENCY_LEVEL
                    queries actually run concurrently per cluster;
                    excess queues; multi-cluster adds more clusters)
                                 │
                                 ▼
                    AsyncJob.result() / future.result() — fan-in,
                    retries, exception aggregation in the driver
```

The driver plays the role of Spark's DAGScheduler in miniature: it decides how many partitions to create and what predicate defines each one (HASH/MOD, date range, or a pre-materialized partition-key table), then hands each partition to a worker. The "executors" are not separate machines — they are the warehouse's own internal nodes, invisible to the client, reached indirectly through however many concurrent queries the warehouse can actually run at once (`MAX_CONCURRENCY_LEVEL`, default 8/cluster). This is the key architectural divergence from Spark: **the driver's fan-out and the warehouse's real physical parallelism are two independent layers**, and the design must respect both — over-fanning past `MAX_CONCURRENCY_LEVEL` just grows a server-side queue, not real concurrency. For CPU-bound per-row Python work (not pure SQL predicates), replace the "one query per partition" leaf with a single vectorized UDTF call using `PARTITION BY`, letting Snowflake's own row-redistribution mechanism do the multi-node fan-out instead of the client. For workloads needing arbitrary runtimes, GPUs, or long-lived stateful workers, replace the whole thread-pool layer with SPCS `EXECUTE JOB SERVICE ... REPLICAS=N`, where the "coordinator" role is simply Snowflake's own service scheduler.

## 7. Minimal Implementation

This is the practical, ship-today answer: a `parallel_map`-style abstraction in well under 100 lines, built on `ThreadPoolExecutor` and one shared thread-safe `Session`.

```python
"""minimal_parallel_map.py — the smallest practical Spark-like parallel_map for Snowpark."""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, TypeVar

from snowflake.snowpark import Session

T = TypeVar("T")
R = TypeVar("R")


def make_session() -> Session:
    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        # Required for real thread safety; defaults to False if omitted.
        # Requires Snowpark Python >= 1.24.0 and server >= 8.46.
        "session_parameters": {"PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION": True},
    }
    return Session.builder.configs(connection_parameters).create()


def parallel_map(
    session: Session,
    items: Iterable[T],
    fn: Callable[[Session, T], R],
    max_workers: int = 8,
) -> list[R]:
    """Run fn(session, item) for every item concurrently on a shared thread-safe
    Session. fn must not mutate session-level config (USE DATABASE/SCHEMA/
    WAREHOUSE/ROLE) and must not share a single AsyncJob across threads."""
    results: list[R] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fn, session, item): item for item in items}
        for future in as_completed(futures):
            results.append(future.result())  # raises here if fn(item) raised
    return results


if __name__ == "__main__":
    session = make_session()

    NUM_PARTITIONS = 8

    def process_partition(session: Session, partition_id: int) -> tuple[int, int]:
        df = session.sql(
            f"SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), {NUM_PARTITIONS}) = {partition_id}"
        )
        row_count = len(df.collect())
        return partition_id, row_count

    results = parallel_map(session, range(NUM_PARTITIONS), process_partition, max_workers=NUM_PARTITIONS)
    for partition_id, row_count in sorted(results):
        print(f"partition {partition_id}: {row_count} rows")

    session.close()
```

This is deliberately small: no retries, no backpressure, no partial-failure recovery. `as_completed` plus `future.result()` re-raises the first exception encountered on `.result()`, which is enough to fail loudly rather than silently drop a partition's data — an acceptable, honest failure mode for a first implementation, but not a production one (Section 8 fixes this).

## 8. Production-Grade Implementation

```python
"""production_parallel_execute.py — parallel_execute with retries, worker caps,
session-pool option, cancellation, structured logging, and basic metrics."""
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass, field
from threading import Semaphore
from typing import Callable, Iterable, Optional, TypeVar

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

logger = logging.getLogger("parallel_execute")

T = TypeVar("T")
R = TypeVar("R")


def build_connection_parameters(**overrides) -> dict:
    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "session_parameters": {"PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION": True},
    }
    params.update(overrides)
    return params


@dataclass
class ExecutionResult:
    item: object
    value: object = None
    error: Optional[BaseException] = None
    attempts: int = 0
    duration_s: float = 0.0

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class Metrics:
    submitted: int = 0
    succeeded: int = 0
    failed: int = 0
    retried: int = 0
    total_duration_s: float = 0.0
    lock: object = field(default_factory=Semaphore)  # cheap counter guard


class ParallelExecutor:
    """Production parallel_execute for Snowpark.

    session_mode:
      "shared"      -> one thread-safe Session shared by all workers (default,
                        matches Snowflake's own documented recommendation).
      "pool"        -> a fixed pool of N independent Sessions, round-robined
                        across workers -- use only when workers need isolated
                        transactions or per-worker session configuration.
    """

    def __init__(
        self,
        connection_parameters: dict,
        max_workers: int = 8,
        session_mode: str = "shared",
        max_retries: int = 3,
        retry_backoff_s: float = 1.0,
        per_task_timeout_s: Optional[float] = 300.0,
    ):
        self.max_workers = max_workers
        self.session_mode = session_mode
        self.max_retries = max_retries
        self.retry_backoff_s = retry_backoff_s
        self.per_task_timeout_s = per_task_timeout_s
        self.metrics = Metrics()
        self._cancelled = False

        if session_mode == "shared":
            self._sessions = [Session.builder.configs(connection_parameters).create()]
        elif session_mode == "pool":
            self._sessions = [
                Session.builder.configs(connection_parameters).create()
                for _ in range(max_workers)
            ]
        else:
            raise ValueError(f"unknown session_mode: {session_mode}")

    def _session_for(self, worker_index: int) -> Session:
        return self._sessions[worker_index % len(self._sessions)]

    def cancel(self) -> None:
        """Cooperative cancellation: in-flight AsyncJobs are cancelled; queued
        (not-yet-started) tasks are skipped."""
        self._cancelled = True

    def close(self) -> None:
        for s in self._sessions:
            try:
                s.close()
            except Exception:
                logger.exception("error closing session")

    def _run_one(
        self, worker_index: int, item: T, fn: Callable[[Session, T], R]
    ) -> ExecutionResult:
        session = self._session_for(worker_index)
        result = ExecutionResult(item=item)
        start = time.monotonic()

        for attempt in range(1, self.max_retries + 1):
            if self._cancelled:
                result.error = RuntimeError("execution cancelled before start")
                return result
            result.attempts = attempt
            try:
                async_job = None
                value = fn(session, item)
                # If fn returns an AsyncJob (e.g. via collect_nowait), resolve it
                # here so cancellation/timeout logic has a handle to act on.
                from snowflake.snowpark.async_job import AsyncJob  # local import
                if isinstance(value, AsyncJob):
                    async_job = value
                    if self.per_task_timeout_s is not None:
                        deadline = time.monotonic() + self.per_task_timeout_s
                        while not async_job.is_done():
                            if self._cancelled:
                                async_job.cancel()
                                raise RuntimeError("cancelled while awaiting AsyncJob")
                            if time.monotonic() > deadline:
                                async_job.cancel()
                                raise TimeoutError(f"task for {item!r} exceeded {self.per_task_timeout_s}s")
                            time.sleep(0.25)
                    value = async_job.result()
                result.value = value
                result.duration_s = time.monotonic() - start
                return result
            except (SnowparkSQLException, TimeoutError, RuntimeError) as exc:
                logger.warning(
                    "task failed (item=%r, attempt=%d/%d): %s",
                    item, attempt, self.max_retries, exc,
                )
                self.metrics.retried += 1
                if attempt >= self.max_retries:
                    result.error = exc
                    result.duration_s = time.monotonic() - start
                    return result
                time.sleep(self.retry_backoff_s * (2 ** (attempt - 1)))  # exponential backoff
            except Exception as exc:  # non-retryable
                logger.exception("non-retryable task failure (item=%r)", item)
                result.error = exc
                result.duration_s = time.monotonic() - start
                return result

        return result  # unreachable, satisfies type checkers

    def run(
        self,
        items: Iterable[T],
        fn: Callable[[Session, T], R],
        max_in_flight: Optional[int] = None,
    ) -> list[ExecutionResult]:
        """Runs fn(session, item) for every item with bounded concurrency
        (backpressure), retries, and per-task timeout/cancellation."""
        max_in_flight = max_in_flight or self.max_workers
        results: list[ExecutionResult] = []
        items = list(items)
        self.metrics.submitted = len(items)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            in_flight: dict[Future, T] = {}
            item_iter = iter(enumerate(items))

            def submit_next():
                try:
                    idx, item = next(item_iter)
                except StopIteration:
                    return False
                future = pool.submit(self._run_one, idx % self.max_workers, item, fn)
                in_flight[future] = item
                return True

            # Prime the pipeline up to the backpressure limit.
            for _ in range(min(max_in_flight, len(items))):
                submit_next()

            while in_flight:
                for future in as_completed(list(in_flight.keys())):
                    del in_flight[future]
                    result = future.result()  # _run_one never raises; it returns ExecutionResult
                    results.append(result)
                    if result.ok:
                        self.metrics.succeeded += 1
                    else:
                        self.metrics.failed += 1
                    self.metrics.total_duration_s += result.duration_s
                    submit_next()
                    break  # re-evaluate in_flight after each completion (backpressure)

        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    conn_params = build_connection_parameters()
    executor = ParallelExecutor(conn_params, max_workers=8, session_mode="shared", max_retries=3)

    NUM_PARTITIONS = 16

    def process_partition(session: Session, partition_id: int):
        df = session.sql(
            f"SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), {NUM_PARTITIONS}) = {partition_id}"
        )
        return len(df.collect())

    try:
        results = executor.run(range(NUM_PARTITIONS), process_partition, max_in_flight=8)
        for r in sorted(results, key=lambda r: r.item):
            status = "OK" if r.ok else f"FAILED: {r.error}"
            print(f"partition {r.item}: {status} ({r.attempts} attempts, {r.duration_s:.2f}s)")
        print(executor.metrics)
    finally:
        executor.close()
```

This extends the minimal version with: **retries** (exponential backoff, capped attempts, distinguishing retryable `SnowparkSQLException`/timeout from other exceptions); **worker limits and backpressure** (`max_in_flight` bounds how many tasks are queued at once, refilling only as slots free up, rather than submitting the entire item list at once); a **session-pool option** (`session_mode="pool"`) for the documented case where workers need isolated transactions or configuration; **cancellation** (`cancel()` flips a cooperative flag checked before each attempt and inside any `AsyncJob` wait loop, and calls `AsyncJob.cancel()` on in-flight jobs); **structured logging** on every retry and failure; and a simple **metrics** object (submitted/succeeded/failed/retried/total duration) that a real deployment would export to Prometheus/StatsD instead of printing.

## 9. Spark-to-Snowpark Mapping

| Spark concept | Snowflake/Snowpark equivalent | One-line explanation |
|---|---|---|
| Driver | The Python process running your Snowpark client code (no server-side analog) | Builds/schedules work client-side, but Snowflake's engine — not the driver — does query planning once SQL is submitted. |
| Executor | Warehouse compute nodes (invisible, not addressable) | Snowflake's MPP engine distributes one query across a cluster's nodes automatically; there is no client-visible executor object or API. |
| Partition | No general equivalent; closest is a vectorized UDTF's `PARTITION BY` group, or a manual HASH/MOD SQL bucket | Snowflake's micro-partitions are a storage/pruning unit, never exposed to Snowpark code as a schedulable partition. |
| Task | No general equivalent; closest is one submitted query/async job, or one UDTF partition invocation | Snowflake has no client-visible task graph; "how many tasks" is not a concept the client controls. |
| Stage | No equivalent | Query staging/optimization happens entirely inside Snowflake's own optimizer, invisible to the client. |
| Shuffle | No equivalent exposed to the client | Any data redistribution needed for joins/aggregations happens inside Snowflake's engine with no client-visible shuffle mechanics or tuning knob. |
| `mapPartitions` | Vectorized UDTF with `end_partition` + `PARTITION BY` | The one documented, intentional analog — a whole logical partition delivered as a pandas DataFrame to one handler invocation. |
| `parallelize()` | No equivalent | Snowpark has no operation that takes a client-side in-memory collection and distributes it as partitioned server-side compute the way `sc.parallelize()` does. |
| Spark scheduler (DAGScheduler/TaskScheduler) | Snowflake's query optimizer/execution engine (opaque) | Snowflake plans and schedules query execution internally; Snowpark Connect for Spark explicitly treats Spark's own scheduling knobs as no-ops. |
| Lazy evaluation | DataFrame API is lazy until an action (`.collect()`, `.show()`, `.to_pandas()`) | Shared root idea with Spark — transformations build a plan, actions trigger execution — but Snowflake's plan optimization is entirely server-side and opaque. |

## 10. Benchmarks

No live benchmark against a real Snowflake account was executed in this research session — no Snowflake account access was available, so the originally proposed Phase 8/9 live 100M-row experiments could not be run. What follows are the real, cited numbers this research did find, plus a harness the reader can run themselves.

**Concurrency contention vs. queuing (datageek.blog, most granular published breakdown found).** A practitioner ran 24 concurrent queries (8 light/8 medium/8 heavy) on an X-Small warehouse at the default `MAX_CONCURRENCY_LEVEL` of 8, and found concurrent queries ran slower than in isolation — but only **5.8%** of that slowdown was attributable to queuing (`QUEUED_OVERLOAD_TIME`); **94.2%** came from resource contention during actual execution, with 21 of 24 queries showing zero queue time at all ([Performance Impacts of Concurrency](https://datageek.blog/2026/04/21/performance-impacts-of-concurrency/)). This directly supports Snowflake's own architectural statement that concurrent queries on one warehouse "share available physical warehouse resources, with each getting a fractional share" ([Snowflake Concurrency Fundamentals](https://medium.com/snowflake/snowflake-concurrency-fundamentals-7a2279dd4c51)) — the warehouse's compute is a fixed pool that fan-out reslices, not multiplies, unless paired with more compute.

**The dbt geospatial case study (the clearest real win from manual fan-out).** A monolithic geospatial join (millions of US residents against ~300,000 points of interest) took **over 24 hours** as a single query. Partitioning it by geographic region and running the partitions concurrently via a Python orchestration script cut runtime by roughly **50%**, at **roughly the same credit consumption** ([dbt Discourse](https://discourse.getdbt.com/t/breaking-a-large-query-into-parallelized-partitioned-queries/4481)). This is a single practitioner's project, not a controlled A/B benchmark, and the win plausibly reflects the single query's own optimizer plan being suboptimal for that data's skew — not a generalizable "fan-out beats native SQL" rule.

**Vectorized UDF speedup.** Vectorized (batched, pandas-based) Python UDFs are documented to give **30–40% improvement for numerical computations** versus row-by-row scalar UDFs, though the advantage is use-case dependent ([Getting the Most From Your Snowpark UDFs, Hakkoda](https://hakkoda.io/resources/snowpark-udf/); corroborated by [Snowflake docs](https://docs.snowflake.cn/en/developer-guide/udf/python/udf-python-batch)).

**Snowflake's own XGBoost/batch-inference cross-platform benchmark.** Comparing Snowflake Batch Inference Jobs on SPCS against Databricks Spark UDF and Amazon SageMaker Batch Transform, on matched hardware (8 nodes × 4 vCPU for the XGBoost case), Snowflake reported **5.68M rows/sec** throughput on a **10-billion-row** structured-data inference workload, and **up to 6.5x higher throughput and up to 5.8x lower compute cost per million rows** versus the other two platforms (methodology: median of three runs, server-side runtime only) ([Snowflake Engineering Blog: Batch Inference Performance](https://www.snowflake.com/en/blog/engineering/snowflake-batch-inference-performance/)).

**The `collect_nowait()`/`AsyncJob` benchmark.** 6 independent operations submitted within <200ms of each other ran in ~9 minutes sequentially versus ~2 minutes in parallel, bounded by the single longest query ([Cloudyard](https://cloudyard.in/2025/10/parallel-jobs-with-snowpark-collect_nowait-async_job/)).

**Explicit open questions — no public benchmark data exists for these, per the research notes:** (1) no controlled, apples-to-apples comparison of "N concurrent partitioned Snowpark queries" vs. "1 big native query" at tens-of-millions-of-rows scale with matched runtime AND credit numbers on both sides; (2) no benchmark of HASH/MOD partitioning specifically (the one numeric benchmark found, dbt's, used manual geographic partitioning, not HASH); (3) no public comparison of "fetch-to-client + local process pool" vs. "N concurrent Python stored-procedure calls" vs. "vectorized UDF" for CPU-bound per-row work; (4) no numeric benchmark for the external-API-call case (network-bound Python enrichment) — the one relevant Snowflake blog on this is architectural only, with no measured throughput; (5) no absolute query-compilation-time figures (ms/seconds) exist publicly to compute a break-even point for how many small queries' compilation overhead cancels a fan-out win; (6) the 84% wall-clock reduction claimed by a "Parallel Model Training" Medium post could not be verified — the source page returned HTTP 403 and the figure is drawn only from a search-engine snippet, not a verified full read; treat it as unconfirmed color, not evidence.

### A ready-to-run benchmark harness

The production implementation from Section 8 can be reused directly as the harness. Run it in three configurations against your own account and warehouse, then read the real numbers from `ACCOUNT_USAGE.QUERY_HISTORY` and `WAREHOUSE_LOAD_HISTORY` rather than trusting client-side wall-clock time alone (client-side overlap of HTTP calls does not by itself prove server-side concurrent execution):

```python
import time
from production_parallel_execute import ParallelExecutor, build_connection_parameters

def benchmark(num_partitions: int, max_workers: int, table: str):
    conn_params = build_connection_parameters()
    executor = ParallelExecutor(conn_params, max_workers=max_workers)

    def process_partition(session, partition_id):
        return session.sql(
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE MOD(ABS(HASH(customer_id)), {num_partitions}) = {partition_id}"
        ).collect()

    t0 = time.monotonic()
    results = executor.run(range(num_partitions), process_partition, max_in_flight=max_workers)
    elapsed = time.monotonic() - t0
    executor.close()
    return elapsed, results

# Arm A: native single query (baseline)
# Arm B: fan-out with max_workers <= MAX_CONCURRENCY_LEVEL (no server-side queuing expected)
# Arm C: fan-out with max_workers > MAX_CONCURRENCY_LEVEL (expect queuing)
for arm, workers in [("A (baseline, 1 query)", 1), ("B (<=MAX_CONCURRENCY_LEVEL)", 8), ("C (>MAX_CONCURRENCY_LEVEL)", 32)]:
    elapsed, _ = benchmark(num_partitions=workers if workers > 1 else 1, max_workers=workers, table="sales")
    print(f"{arm}: {elapsed:.2f}s wall clock")
```

```sql
-- After each run, pull the ground truth server-side numbers for that time window.
SELECT query_id, warehouse_name, execution_status, total_elapsed_time,
       compilation_time, queued_provisioning_time, queued_overload_time, bytes_scanned
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%HASH(customer_id)%'
  AND start_time > DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY start_time;

SELECT start_time, avg_running, avg_queued_load, avg_queued_provisioning, avg_blocked
FROM snowflake.account_usage.warehouse_load_history
WHERE warehouse_name = 'MY_WH'
  AND start_time > DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY start_time;
```

Note that `WAREHOUSE_LOAD_HISTORY` has up to roughly 180 minutes of ingestion latency, so treat it as retrospective, not real-time confirmation.

## 11. Limitations

**`ThreadPoolExecutor` + shared Session.** Requires Snowpark Python ≥1.24.0 and server ≥8.46 plus an explicit session-parameter opt-in that defaults to off — a connector/client-version check alone is not sufficient proof of real thread safety, because the server-side parameter gate could silently be absent on an older account deployment (in which case `Session` falls back to inert `DummyRLock`/`DummyThreadLocal` objects with **no real thread isolation**, per source: [_internal/utils.py#L960-L985](https://github.com/snowflakedb/snowpark-python/blob/main/src/snowflake/snowpark/_internal/utils.py#L960-L985)). It cannot run concurrent transactions on one session, cannot tolerate concurrent session-configuration changes, and its effective parallelism is hard-capped by `MAX_CONCURRENCY_LEVEL` regardless of thread count — spawning more threads than the warehouse can run concurrently just grows a server-side queue.

**`multiprocessing`/`ProcessPoolExecutor`.** Explicitly unsupported inside Snowflake-side Python handlers; on the client side it is unbenchmarked, adds process-startup and per-process authentication overhead, and offers no GIL advantage for an I/O-bound workload — there is no evidence it ever outperforms threading for this use case.

**Multiple Sessions (one per worker).** Multiplies authentication/connection overhead linearly with worker count; no source quantifies where this overhead becomes material, and no published benchmark compares it against a shared session for throughput.

**HASH/MOD SQL fan-out.** The `HASH()` function is computed on the fly for every row, a real and named cost on very large tables; it is unbenchmarked against range/domain partitioning, and no source ties partition boundaries to Snowflake's own micro-partition clustering for pruning efficiency — that alignment is a plausible optimization, not a documented one.

**`collect_nowait()`/`AsyncJob`.** Submission is still one blocking HTTP round trip per call from a single thread — N submissions in a tight loop from one thread are N sequential round trips, not instantaneous; `.result()` on a single `AsyncJob` instance is not safe to call from multiple threads concurrently (unlocked cursor-state mutation); and it inherits all the same warehouse-concurrency ceilings as any other query.

**Scripting `ASYNC`/`AWAIT`.** SQL-only — unusable from a Python handler; hard-capped at 4,000 concurrent child jobs; explicitly does not support fire-and-forget (a job still running when its parent finishes is auto-canceled), so it cannot back a persistent, detached background job.

**Task DAGs.** Seconds-scale per-transition overhead (cold starts commonly 5–10s, sometimes 15–30s) makes them unsuitable for fine-grained fan-out of many small units; DAGs run sequentially by default unless explicit parallel edges are defined; the child-task-count ceiling (commonly cited as ~100) was not independently confirmed against Snowflake's primary documentation in this research.

**Vectorized UDFs/UDTFs.** Batch size for plain vectorized UDFs is only an upper bound the engine may ignore — not a performance-tuning lever the way Spark's partition count is; no partition-identity signal is exposed (no `mapPartitionsWithIndex` equivalent); handler execution is capped at 180 seconds; and no documented mechanism exists for cross-batch or cross-partition communication during execution.

**Snowpark Container Services.** The heaviest-weight option: requires building/maintaining container images, managing compute-pool sizing and lifecycle, and understanding a materially different, hourly/per-node billing model distinct from per-query warehouse billing; it is architecturally the closest analog to Spark but is explicitly framed by Snowflake and practitioners as reserved for workloads UDFs structurally can't cover (arbitrary runtimes, GPUs, long-running stateful services) — not a default choice for ordinary batch transformations.

**Streams/Tasks as a work queue.** Not a documented or proven pattern at all — Snowflake's own recommended fan-out mechanism for Streams is one-stream-per-consumer, not competing consumers on a shared queue; a SKIP LOCKED-style pattern would need to be hand-built on plain tables and has no confirmed safety guarantees under concurrent `UPDATE`/`MERGE` from multiple task instances.

**Across every mechanism.** No public source provides a controlled, apples-to-apples benchmark of fan-out vs. single-query at real scale with both runtime and credits reported for both arms — every quantitative claim in Section 10 comes from a different workload, warehouse size, and methodology, and none of them should be treated as a general multiplier applicable to an arbitrary reader's workload.

## 12. Snowpark Source-Code Analysis

This section is drawn directly from the primary-source investigation in the research notes, which cloned `github.com/snowflakedb/snowpark-python` (commit `5ab7287b7f4276f651df6b1257b167802cda7251`) and `github.com/snowflakedb/snowflake-connector-python` (commit `963040f03abe646fdd8cda03f9bbb0ff15209b49`) as of 2026-09-29. Line numbers may drift as the repos evolve but were accurate as of those commits.

**Thread safety is real, gated, and locally scoped — CONFIRMED.** `Session`'s own docstring states it is thread-safe "since version 1.24.0 or later," requiring server version 8.46+ ([session.py#L401-L410](https://github.com/snowflakedb/snowpark-python/blob/main/src/snowflake/snowpark/session.py#L401-L410)), added via [snowpark-python#4372, "Document thread safety in the Session class docstring"](https://github.com/snowflakedb/snowpark-python/pull/4372) — a documentation-only PR whose description states the API reference previously "contained no mention of threading" and the change followed customer feedback. This is not merely a documentation claim: it is implemented with real per-instance `RLock`s (`self._lock`, `self._package_lock`, `self._plan_lock`, `self._xpath_udf_cache_lock`, `self._cte_error_lock`, [session.py#L860-L890](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/session.py#L860-L890)) and a **thread-local cursor**: `self._cursor` on `ServerConnection` is a property that does `if not hasattr(self._thread_store, "cursor"): self._thread_store.cursor = self._conn.cursor()` ([server_connection.py#L210-L216](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L210-L216)) — each thread gets its own dedicated `SnowflakeCursor`, created once and cached. The gate itself is a server-provided client-side session parameter, `PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION`, read via `self._get_client_side_session_parameter(..., False)` — defaulting to `False` if the server doesn't return it ([server_connection.py#L190-L195](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/_internal/server_connection.py#L190-L195)). When disabled, `create_rlock()`/`create_thread_local()` return no-op `DummyRLock`/`DummyThreadLocal` objects instead of real primitives ([_internal/utils.py#L960-L985](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/_internal/utils.py#L960-L985)) — meaning a recent client version connected to an account that hasn't rolled out the server-side flag gets **silently no real thread isolation**, a meaningful risk this report flags as CONFIRMED-by-code but unverifiable as "universally on" without a live `SHOW PARAMETERS` check.

**The unlocked `cmd_query()` mutation — CONFIRMED.** `SnowflakeConnection.cmd_query()` unconditionally mutates shared connection attributes — `self._database`, `self._schema`, `self._warehouse`, `self._role` — after every query response, **with no lock around these assignments** (`_update_current_object=True` by default) ([connection.py#L2091-L2102](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/connection.py#L2091-L2102)). This is the concrete mechanism behind the documented warning that changing session configuration from one thread while another queries is unsafe — it is not an abstract caveat, it is this exact unguarded write.

**`AsyncJob.result()` is explicitly documented internally as not thread-safe — CONFIRMED.** An internal code comment states: *"AsyncJob.result() is not thread-safe — the underlying connector cursor mutates shared state (`_result`, `_rownumber`, `_prefetch_hook`) during result fetching, so only one thread may call it"* ([session.py#L5150-L5155](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/session.py#L5150-L5155)). Because each `AsyncJob` gets its own fresh cursor in `__init__` (`self._cursor = session._conn._conn.cursor()`, [async_job.py#L184-L209](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/snowpark/async_job.py)), the hazard is specific to calling `.result()` on the *same* `AsyncJob` object from multiple threads, not to holding many different `AsyncJob`s concurrently.

**No lock around the HTTP call path itself — CONFIRMED, supports genuine client-side concurrency.** The connector's `SessionManager` maintains a per-hostname pool of reusable `requests.Session` objects (`SessionPool`), and `SessionManager.request()` acquires-and-releases one around each blocking call ([session_manager.py#L176-L230](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/session_manager.py#L176-L230), [session_manager.py#L529-L546](https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/session_manager.py#L529-L546)). No `Lock`/`RLock`/`Semaphore` guards the request itself — the only locks found in the connector guard narrow bookkeeping (`_lock_sequence_counter`, `_lock_converter`, `_lock_canceling`). This confirms N threads calling `.collect()`/`.collect_nowait()` on one thread-safe `Session` genuinely issue N concurrent HTTP requests — real client-side overlap, though server-side execution concurrency is separately gated by warehouse capacity.

**A real, open thread-safety GitHub issue exists — but not on the classic path.** [snowpark-python#3281, "SNOW-2046081: Library is not threadsafe when multiple sessions/roots exist due to globally shared state"](https://github.com/snowflakedb/snowpark-python/issues/3281) (open) documents intermittent `401 Unauthorized` errors when a user created multiple `Session`/`Root` objects against different accounts and issued requests concurrently from parallel threads. The root cause traced by the reporter is `snowflake.core.*._generated.ApiClient` and `Configuration` classes storing a `_default` instance as a **class variable** — a globally shared, stomped-on default across sessions once code runs on separate threads. A Snowflake engineer (`sfc-gh-sghosh`) could not reproduce with the given sample, and the issue remains open/unresolved as of this research. A related closed issue, [snowflake-connector-python#1700, "Non Thread Safe Singleton Telemetry Service in Python"](https://github.com/snowflakedb/snowflake-connector-python/issues/1700), documents the same *class* of bug (a shared, non-thread-aware singleton) in the connector's telemetry code — independent confirmation this failure mode has recurred more than once. Both issues concern surface area **adjacent to**, not the same as, the classic `Session`/`DataFrame`/`AsyncJob` execution path that was deliberately re-engineered for thread safety in 1.24.0 — the report's conclusion is that "thread-safe" claims in this ecosystem are locally scoped, and a reader should not assume the whole Snowflake Python client surface (including the newer `snowflake.core.Root` object-management API) is uniformly thread-safe just because `Session`/`DataFrame` are.

## 13. Proposed Snowpark Extension

**PROPOSED — this is a design, not an existing Snowflake feature.** The research is unambiguous that Snowpark falls short of Spark's ergonomics in two specific, narrow ways: there is no native partition-count control over a DataFrame's execution, and there is no built-in `map_partitions()` method on `DataFrame` itself (the only real equivalent, the vectorized UDTF `end_partition`+`PARTITION BY` pattern, requires registering a separate UDTF class and rewriting the call as a `TABLE(...)` SQL expression, rather than a fluent `.map_partitions(fn)` chained onto an existing DataFrame). A small extension library — not a Snowflake platform change, just a well-designed client-side package — could close this gap:

**API shape.**
```python
class ParallelDataFrame:
    """Wraps a snowflake.snowpark.DataFrame plus a partitioning plan."""

    def __init__(self, df: DataFrame, partition_by: str, num_partitions: int):
        self._df = df
        self._partition_by = partition_by
        self._num_partitions = num_partitions

    def map_partitions(self, fn: Callable[[pd.DataFrame], pd.DataFrame]) -> "ParallelDataFrame":
        """Registers fn as a vectorized UDTF end_partition handler under the
        hood and rewrites the query to TABLE(fn_udtf(...) OVER (PARTITION BY ...))."""
        ...

    def collect_parallel(self, max_workers: int = 8) -> list[Row]:
        """Fans out one query per partition via the scheduler below and
        fans results back in, OR — if map_partitions was used — issues a
        single query and lets Snowflake's own row-redistribution parallelize it."""
        ...
```

**Scheduler / partition planner.** A `PartitionPlanner` component decides, given a target table/DataFrame and a requested partition count, whether to (a) emit N `WHERE MOD(ABS(HASH(key)), N) = i` predicates for independent-query fan-out, or (b) emit one `PARTITION BY key` UDTF call for engine-managed fan-out — choosing (b) whenever the per-partition work is expressible as a single Python function over a pandas DataFrame (the common case), and falling back to (a) only when partitions must run genuinely different SQL (e.g., different target tables per partition).

**Worker/session pool.** A `SessionPool` class holding N pre-created, health-checked `Session` objects (built once, reused across many `run()` calls, not per-task) — amortizing the authentication overhead the research notes flag as unquantified but real, and exposing both the "shared session" and "pool" modes from Section 8 through one configuration flag.

**Async query management.** A `JobRegistry` that wraps every `AsyncJob`/future with a uniform handle exposing `.status()`, `.cancel()`, `.result(timeout=...)`, regardless of whether the underlying mechanism is a thread future or a native `AsyncJob` — so the rest of the library never has to know which submission mechanism was used.

**Retries, failure handling, cancellation, observability.** Lift directly from Section 8's `ParallelExecutor`: exponential backoff with a retryable/non-retryable exception split, a cooperative cancellation flag checked at every poll loop and propagated into `AsyncJob.cancel()`, structured logging per attempt, and a `Metrics` object designed from the start to export to OpenTelemetry/Prometheus rather than print statements.

**Warehouse concurrency control.** A `ConcurrencyGovernor` that reads the target warehouse's `MAX_CONCURRENCY_LEVEL` (via `SHOW PARAMETERS LIKE 'MAX_CONCURRENCY_LEVEL' IN WAREHOUSE <wh>`) at startup and automatically caps `max_in_flight` to that value by default — directly preventing the documented failure mode where naively raising client thread count past the warehouse's concurrency ceiling only grows a server-side queue rather than adding real parallelism. An opt-in override would let a user targeting a known multi-cluster/auto-scale warehouse raise the cap deliberately, paired with a warning surfaced from `WAREHOUSE_LOAD_HISTORY`/`QUERY_HISTORY.queued_overload_time` if queuing is subsequently observed.

This is intentionally not a request for a Snowflake platform feature — it is a specification for a thin, honest client library that gives Python developers the `map_partitions`-shaped ergonomics they're used to from Spark, while being transparent that all the actual parallel compute still comes from the same mechanisms documented in Sections 3–5: thread-safe sessions, `AsyncJob`, and vectorized UDTFs.

## 14. Final Recommended Implementation

The smallest implementation to copy, paste, and use immediately — restated cleanly from Section 7:

```python
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from snowflake.snowpark import Session

def make_session() -> Session:
    return Session.builder.configs({
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        # REQUIRED for real thread safety. Defaults to False.
        # Needs Snowpark Python >= 1.24.0 and server >= 8.46.
        "session_parameters": {"PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION": True},
    }).create()

def parallel_map(session, items, fn, max_workers=8):
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fn, session, item): item for item in items}
        return [f.result() for f in as_completed(futures)]

# Usage: process an 8-way HASH-partitioned table concurrently.
if __name__ == "__main__":
    session = make_session()
    N = 8

    def process(session, partition_id):
        rows = session.sql(
            f"SELECT * FROM sales WHERE MOD(ABS(HASH(customer_id)), {N}) = {partition_id}"
        ).collect()
        return partition_id, len(rows)

    for partition_id, count in sorted(parallel_map(session, range(N), process, max_workers=N)):
        print(f"partition {partition_id}: {count} rows")

    session.close()
```

Three rules make this safe: keep `max_workers` at or below the target warehouse's `MAX_CONCURRENCY_LEVEL` (default 8) so fan-out gains real parallelism instead of server-side queuing; never call `USE DATABASE`/`SCHEMA`/`WAREHOUSE`/`ROLE` or start a transaction from one worker while others are running; and never call `.result()` on the same `AsyncJob` from more than one thread. Reach for this only when the unit of work is genuinely independent and not itself expressible as one SQL statement — for ordinary aggregations and joins, submit one query and let Snowflake's own distributed engine do what it already does automatically.
