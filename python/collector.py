import argparse
from typing import Dict

from sf_bootstrap import ensure_copilot_bootstrap, get_conn



def _exec(cur, sql: str, params=None):
    cur.execute(sql, params or {})


def _collect_query_facts(cur, days: int):
    _exec(cur, "TRUNCATE TABLE COST_COPILOT.STG_QUERY_HISTORY")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.STG_QUERY_HISTORY (
          query_id, query_tag, warehouse_name, user_name, role_name, start_time, end_time,
          total_elapsed_time, execution_time, compilation_time, bytes_scanned,
          partitions_scanned, partitions_total, bytes_spilled_to_local_storage,
          bytes_spilled_to_remote_storage, queued_overload_time, queued_provisioning_time, query_text
        )
        SELECT
          query_id, query_tag, warehouse_name, user_name, role_name, start_time, end_time,
          total_elapsed_time, execution_time, compilation_time, bytes_scanned,
          partitions_scanned, partitions_total, bytes_spilled_to_local_storage,
          bytes_spilled_to_remote_storage, queued_overload_time, queued_provisioning_time, query_text
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
        """,
        {"days": days},
    )
    _exec(
        cur,
        """
        MERGE INTO COST_COPILOT.FACT_QUERY t
        USING COST_COPILOT.STG_QUERY_HISTORY s
        ON t.query_id = s.query_id
        WHEN MATCHED THEN UPDATE SET
          query_tag = s.query_tag,
          warehouse_name = s.warehouse_name,
          user_name = s.user_name,
          role_name = s.role_name,
          start_time = s.start_time,
          end_time = s.end_time,
          total_elapsed_time = s.total_elapsed_time,
          execution_time = s.execution_time,
          compilation_time = s.compilation_time,
          bytes_scanned = s.bytes_scanned,
          partitions_scanned = s.partitions_scanned,
          partitions_total = s.partitions_total,
          bytes_spilled_to_local_storage = s.bytes_spilled_to_local_storage,
          bytes_spilled_to_remote_storage = s.bytes_spilled_to_remote_storage,
          queued_overload_time = s.queued_overload_time,
          queued_provisioning_time = s.queued_provisioning_time,
          query_text = s.query_text,
          load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          query_id, query_tag, warehouse_name, user_name, role_name, start_time, end_time,
          total_elapsed_time, execution_time, compilation_time, bytes_scanned,
          partitions_scanned, partitions_total, bytes_spilled_to_local_storage,
          bytes_spilled_to_remote_storage, queued_overload_time, queued_provisioning_time, query_text
        ) VALUES (
          s.query_id, s.query_tag, s.warehouse_name, s.user_name, s.role_name, s.start_time, s.end_time,
          s.total_elapsed_time, s.execution_time, s.compilation_time, s.bytes_scanned,
          s.partitions_scanned, s.partitions_total, s.bytes_spilled_to_local_storage,
          s.bytes_spilled_to_remote_storage, s.queued_overload_time, s.queued_provisioning_time, s.query_text
        )
        """,
    )


def _collect_cost_facts(cur, days: int):
    _exec(cur, "TRUNCATE TABLE COST_COPILOT.STG_QUERY_ATTRIBUTION_HISTORY")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.STG_QUERY_ATTRIBUTION_HISTORY (
          query_id, query_tag, start_time, credits_attributed_compute, credits_used_query_acceleration
        )
        SELECT
          query_id, query_tag, start_time, credits_attributed_compute, credits_used_query_acceleration
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
        WHERE start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
        """,
        {"days": days},
    )
    _exec(
        cur,
        """
        MERGE INTO COST_COPILOT.FACT_QUERY_COST t
        USING COST_COPILOT.STG_QUERY_ATTRIBUTION_HISTORY s
        ON t.query_id = s.query_id
        WHEN MATCHED THEN UPDATE SET
          query_tag = s.query_tag,
          start_time = s.start_time,
          credits_attributed_compute = s.credits_attributed_compute,
          credits_used_query_acceleration = s.credits_used_query_acceleration,
          load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          query_id, query_tag, start_time, credits_attributed_compute, credits_used_query_acceleration
        ) VALUES (
          s.query_id, s.query_tag, s.start_time, s.credits_attributed_compute, s.credits_used_query_acceleration
        )
        """,
    )


def _collect_warehouse_facts(cur, days: int):
    _exec(cur, "TRUNCATE TABLE COST_COPILOT.STG_WAREHOUSE_METERING_HOURLY")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.STG_WAREHOUSE_METERING_HOURLY (
          warehouse_name, start_time, end_time, credits_used, credits_used_compute,
          credits_used_cloud_services, avg_running, avg_queued_load, avg_queued_provisioning, avg_blocked
        )
        SELECT
          m.warehouse_name,
          m.start_time,
          m.end_time,
          m.credits_used,
          m.credits_used_compute,
          m.credits_used_cloud_services,
          l.avg_running,
          l.avg_queued_load,
          l.avg_queued_provisioning,
          l.avg_blocked
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY m
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY l
          ON m.warehouse_name = l.warehouse_name
         AND m.start_time = l.start_time
        WHERE m.start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
          AND m.warehouse_name IS NOT NULL
        """,
        {"days": days},
    )
    _exec(
        cur,
        """
        MERGE INTO COST_COPILOT.FACT_WAREHOUSE_HOURLY t
        USING COST_COPILOT.STG_WAREHOUSE_METERING_HOURLY s
        ON t.warehouse_name = s.warehouse_name
       AND t.start_time = s.start_time
        WHEN MATCHED THEN UPDATE SET
          end_time = s.end_time,
          credits_used = s.credits_used,
          credits_used_compute = s.credits_used_compute,
          credits_used_cloud_services = s.credits_used_cloud_services,
          avg_running = s.avg_running,
          avg_queued_load = s.avg_queued_load,
          avg_queued_provisioning = s.avg_queued_provisioning,
          avg_blocked = s.avg_blocked,
          load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          warehouse_name, start_time, end_time, credits_used, credits_used_compute,
          credits_used_cloud_services, avg_running, avg_queued_load, avg_queued_provisioning, avg_blocked
        ) VALUES (
          s.warehouse_name, s.start_time, s.end_time, s.credits_used, s.credits_used_compute,
          s.credits_used_cloud_services, s.avg_running, s.avg_queued_load, s.avg_queued_provisioning, s.avg_blocked
        )
        """,
    )


def _collect_pipe_facts(cur, days: int):
    _exec(cur, "TRUNCATE TABLE COST_COPILOT.STG_PIPE_DAILY")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.STG_PIPE_DAILY (
          usage_date, pipe_name, credits_used, files_inserted, bytes_inserted, rows_inserted,
          copy_failure_count, copy_success_count, small_file_loads
        )
        WITH pipe_usage AS (
          SELECT
            DATE(START_TIME) AS usage_date,
            PIPE_NAME AS pipe_name,
            SUM(CREDITS_USED) AS credits_used,
            SUM(FILES_INSERTED) AS files_inserted,
            SUM(BYTES_INSERTED) AS bytes_inserted
          FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
          WHERE START_TIME >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
          GROUP BY 1, 2
        ),
        copy_daily AS (
          SELECT
            DATE(LAST_LOAD_TIME) AS usage_date,
            PIPE_NAME AS pipe_name,
            COUNT_IF(STATUS = 'LOAD_FAILED') AS copy_failure_count,
            COUNT_IF(STATUS = 'LOADED') AS copy_success_count,
            SUM(IFF(STATUS = 'LOADED', COALESCE(ROW_COUNT, 0), 0)) AS rows_inserted,
            COUNT_IF(ROW_COUNT < 1000) AS small_file_loads
          FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
          WHERE LAST_LOAD_TIME >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
          GROUP BY 1, 2
        )
        SELECT
          COALESCE(p.usage_date, c.usage_date) AS usage_date,
          COALESCE(p.pipe_name, c.pipe_name) AS pipe_name,
          COALESCE(p.credits_used, 0) AS credits_used,
          COALESCE(p.files_inserted, 0) AS files_inserted,
          COALESCE(p.bytes_inserted, 0) AS bytes_inserted,
          COALESCE(c.rows_inserted, 0) AS rows_inserted,
          COALESCE(c.copy_failure_count, 0) AS copy_failure_count,
          COALESCE(c.copy_success_count, 0) AS copy_success_count,
          COALESCE(c.small_file_loads, 0) AS small_file_loads
        FROM pipe_usage p
        FULL OUTER JOIN copy_daily c
          ON p.usage_date = c.usage_date
         AND p.pipe_name = c.pipe_name
        WHERE COALESCE(p.pipe_name, c.pipe_name) IS NOT NULL
        """,
        {"days": days},
    )
    _exec(
        cur,
        """
        MERGE INTO COST_COPILOT.FACT_PIPE_DAILY t
        USING COST_COPILOT.STG_PIPE_DAILY s
        ON t.usage_date = s.usage_date
       AND t.pipe_name = s.pipe_name
        WHEN MATCHED THEN UPDATE SET
          credits_used = s.credits_used,
          files_inserted = s.files_inserted,
          bytes_inserted = s.bytes_inserted,
          rows_inserted = s.rows_inserted,
          copy_failure_count = s.copy_failure_count,
          copy_success_count = s.copy_success_count,
          small_file_loads = s.small_file_loads,
          load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          usage_date, pipe_name, credits_used, files_inserted, bytes_inserted, rows_inserted,
          copy_failure_count, copy_success_count, small_file_loads
        ) VALUES (
          s.usage_date, s.pipe_name, s.credits_used, s.files_inserted, s.bytes_inserted, s.rows_inserted,
          s.copy_failure_count, s.copy_success_count, s.small_file_loads
        )
        """,
    )


def _collect_task_facts(cur, days: int):
    _exec(cur, "TRUNCATE TABLE COST_COPILOT.STG_TASK_DAILY")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.STG_TASK_DAILY (
          usage_date, database_name, schema_name, task_name, state, run_count, failed_count, avg_duration_seconds
        )
        SELECT
          DATE(COMPLETED_TIME) AS usage_date,
          DATABASE_NAME,
          SCHEMA_NAME,
          NAME AS task_name,
          STATE,
          COUNT(*) AS run_count,
          COUNT_IF(STATE IN ('FAILED', 'CANCELLED')) AS failed_count,
          AVG(DATEDIFF(SECOND, QUERY_START_TIME, COMPLETED_TIME)) AS avg_duration_seconds
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE COMPLETED_TIME >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
        GROUP BY 1, 2, 3, 4, 5
        """,
        {"days": days},
    )
    _exec(
        cur,
        """
        MERGE INTO COST_COPILOT.FACT_TASK_DAILY t
        USING COST_COPILOT.STG_TASK_DAILY s
        ON t.usage_date = s.usage_date
       AND t.database_name = s.database_name
       AND t.schema_name = s.schema_name
       AND t.task_name = s.task_name
       AND t.state = s.state
        WHEN MATCHED THEN UPDATE SET
          run_count = s.run_count,
          failed_count = s.failed_count,
          avg_duration_seconds = s.avg_duration_seconds,
          load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          usage_date, database_name, schema_name, task_name, state, run_count, failed_count, avg_duration_seconds
        ) VALUES (
          s.usage_date, s.database_name, s.schema_name, s.task_name, s.state, s.run_count, s.failed_count, s.avg_duration_seconds
        )
        """,
    )


def _collect_storage_facts(cur):
    _exec(cur, "TRUNCATE TABLE COST_COPILOT.STG_STORAGE_DAILY")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.STG_STORAGE_DAILY (
          usage_date, table_catalog, table_schema, table_name,
          active_bytes, time_travel_bytes, failsafe_bytes, retained_for_clone_bytes
        )
        SELECT
          CURRENT_DATE() AS usage_date,
          TABLE_CATALOG,
          TABLE_SCHEMA,
          TABLE_NAME,
          ACTIVE_BYTES,
          TIME_TRAVEL_BYTES,
          FAILSAFE_BYTES,
          RETAINED_FOR_CLONE_BYTES
        FROM (
          SELECT
            TABLE_CATALOG,
            TABLE_SCHEMA,
            TABLE_NAME,
            ACTIVE_BYTES,
            TIME_TRAVEL_BYTES,
            FAILSAFE_BYTES,
            RETAINED_FOR_CLONE_BYTES,
            ROW_NUMBER() OVER (
              PARTITION BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
              ORDER BY ACTIVE_BYTES DESC, TIME_TRAVEL_BYTES DESC
            ) AS rn
          FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
          WHERE TABLE_CATALOG IS NOT NULL
            AND TABLE_SCHEMA IS NOT NULL
            AND TABLE_NAME IS NOT NULL
        ) s
        WHERE s.rn = 1
        """
    )
    _exec(
        cur,
        """
        MERGE INTO COST_COPILOT.FACT_STORAGE_DAILY t
        USING COST_COPILOT.STG_STORAGE_DAILY s
        ON t.usage_date = s.usage_date
       AND t.table_catalog = s.table_catalog
       AND t.table_schema = s.table_schema
       AND t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
          active_bytes = s.active_bytes,
          time_travel_bytes = s.time_travel_bytes,
          failsafe_bytes = s.failsafe_bytes,
          retained_for_clone_bytes = s.retained_for_clone_bytes,
          load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          usage_date, table_catalog, table_schema, table_name,
          active_bytes, time_travel_bytes, failsafe_bytes, retained_for_clone_bytes
        ) VALUES (
          s.usage_date, s.table_catalog, s.table_schema, s.table_name,
          s.active_bytes, s.time_travel_bytes, s.failsafe_bytes, s.retained_for_clone_bytes
        )
        """,
    )


def _refresh_dimensions(cur):
    _exec(cur, "DELETE FROM COST_COPILOT.DIM_TIME WHERE date_key >= DATEADD(DAY, -30, CURRENT_DATE())")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_TIME (date_key, year, month, day, week, day_of_week)
        SELECT DISTINCT
          DATE(start_time),
          YEAR(start_time),
          MONTH(start_time),
          DAY(start_time),
          WEEK(start_time),
          DAYOFWEEK(start_time)
        FROM COST_COPILOT.FACT_QUERY
        WHERE start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
        """
    )

    _exec(cur, "TRUNCATE TABLE COST_COPILOT.DIM_JOB")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_JOB (
          job_key, query_tag, tag_source, env, model_name, job_name, dag_id, task_id, run_id
        )
        SELECT
          SHA2(COALESCE(query_tag, 'UNTAGGED')),
          query_tag,
          SPLIT_PART(query_tag, ':', 1) AS tag_source,
          SPLIT_PART(query_tag, ':', 2) AS env,
          REGEXP_SUBSTR(query_tag, 'model=([^:]+)', 1, 1, 'e', 1) AS model_name,
          REGEXP_SUBSTR(query_tag, 'job=([^:]+)', 1, 1, 'e', 1) AS job_name,
          REGEXP_SUBSTR(query_tag, 'dag=([^:]+)', 1, 1, 'e', 1) AS dag_id,
          REGEXP_SUBSTR(query_tag, 'task=([^:]+)', 1, 1, 'e', 1) AS task_id,
          REGEXP_SUBSTR(query_tag, 'run=([^:]+)', 1, 1, 'e', 1) AS run_id
        FROM (
          SELECT DISTINCT query_tag
          FROM COST_COPILOT.FACT_QUERY
          WHERE query_tag IS NOT NULL
        )
        """
    )

    _exec(cur, "TRUNCATE TABLE COST_COPILOT.DIM_WAREHOUSE")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_WAREHOUSE (warehouse_name)
        SELECT DISTINCT warehouse_name
        FROM COST_COPILOT.FACT_QUERY
        WHERE warehouse_name IS NOT NULL
        """
    )

    _exec(cur, "TRUNCATE TABLE COST_COPILOT.DIM_USER")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_USER (user_name)
        SELECT DISTINCT user_name
        FROM COST_COPILOT.FACT_QUERY
        WHERE user_name IS NOT NULL
        """
    )

    _exec(cur, "TRUNCATE TABLE COST_COPILOT.DIM_ROLE")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_ROLE (role_name)
        SELECT DISTINCT role_name
        FROM COST_COPILOT.FACT_QUERY
        WHERE role_name IS NOT NULL
        """
    )

    _exec(cur, "TRUNCATE TABLE COST_COPILOT.DIM_PIPE")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_PIPE (pipe_name)
        SELECT DISTINCT pipe_name
        FROM COST_COPILOT.FACT_PIPE_DAILY
        WHERE pipe_name IS NOT NULL
        """
    )

    _exec(cur, "TRUNCATE TABLE COST_COPILOT.DIM_TASK")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_TASK (task_name, database_name, schema_name)
        SELECT DISTINCT task_name, database_name, schema_name
        FROM COST_COPILOT.FACT_TASK_DAILY
        WHERE task_name IS NOT NULL
        """
    )

    _exec(cur, "TRUNCATE TABLE COST_COPILOT.DIM_TABLE")
    _exec(
        cur,
        """
        INSERT INTO COST_COPILOT.DIM_TABLE (table_key, table_catalog, table_schema, table_name)
        SELECT DISTINCT
          SHA2(table_catalog || '.' || table_schema || '.' || table_name),
          table_catalog,
          table_schema,
          table_name
        FROM COST_COPILOT.FACT_STORAGE_DAILY
        """
    )


def _source_latency(cur) -> Dict[str, str]:
    sources = {
        "QUERY_HISTORY": "SELECT MAX(START_TIME) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY",
        "QUERY_ATTRIBUTION_HISTORY": "SELECT MAX(START_TIME) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY",
        "WAREHOUSE_METERING_HISTORY": "SELECT MAX(START_TIME) FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY",
        "PIPE_USAGE_HISTORY": "SELECT MAX(START_TIME) FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY",
        "TASK_HISTORY": "SELECT MAX(COMPLETED_TIME) FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY",
    }
    delays = {}
    for source_name, sql in sources.items():
        cur.execute(sql)
        value = cur.fetchone()[0]
        delays[source_name] = str(value) if value else "NULL"
    return delays


def run_collector(days: int = 7):
    conn = get_conn()
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            _collect_query_facts(cur, days)
            _collect_cost_facts(cur, days)
            _collect_warehouse_facts(cur, days)
            _collect_pipe_facts(cur, days)
            _collect_task_facts(cur, days)
            _collect_storage_facts(cur)
            _refresh_dimensions(cur)
            conn.commit()
            return _source_latency(cur)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    delays = run_collector(days=args.days)
    print("Collector completed.")
    print("ACCOUNT_USAGE source freshness (max timestamp seen):")
    for k, v in delays.items():
        print(f"  - {k}: {v}")


if __name__ == "__main__":
    main()
