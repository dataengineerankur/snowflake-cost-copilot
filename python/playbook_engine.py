import uuid

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


DEFAULT_PLAYBOOKS = [
    ("HEAVY_JOB", "Heavy Job Tuning", "Query tag attribution exists", "Tune top 3 expensive queries", "Re-model joins/materializations", "Enforce model-level budgets", "SELECT * FROM COST_COPILOT.V_HEAVY_TAGS_7D ORDER BY credits_total DESC LIMIT 20", "MEDIUM"),
    ("MEMORY_PRESSURE", "Spill Reduction", "Spill signals present", "Increase warehouse one size for next run", "Fix skewed joins/aggregations", "Redesign data model for cardinality", "SELECT * FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS WHERE spill_gb > 1", "MEDIUM"),
    ("BAD_PRUNING", "Pruning Improvement", "Pruning ratio available", "Add selective predicates", "Cluster keys/materialized aggregates", "Model partitioning strategy", "SELECT * FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS WHERE pruning_ratio > 0.8", "MEDIUM"),
    ("CONGESTION", "Concurrency Control", "Queue metrics available", "Stagger batch starts", "Enable multi-cluster policy", "Workload orchestration redesign", "SELECT * FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS", "MEDIUM"),
    ("IDLE_BURN", "Idle Burn Control", "Low avg_running", "Lower autosuspend", "Resource monitor tuning", "Capacity planning policy", "SELECT * FROM COST_COPILOT.V_WAREHOUSE_IDLE_SIGNALS", "LOW"),
    ("INGESTION_WASTE", "Pipe Ingestion Efficiency", "Copy/pipe metrics available", "Batch small files upstream", "Retry dead-letter handling", "Data contract on ingest granularity", "SELECT * FROM COST_COPILOT.V_PIPE_HEALTH_SIGNALS", "MEDIUM"),
]


def seed_playbooks() -> int:
    conn = get_conn()
    created = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            for rec_type, name, pre, quick, best, long, vsql, risk in DEFAULT_PLAYBOOKS:
                cur.execute(
                    """
                    MERGE INTO COST_COPILOT.PLAYBOOKS t
                    USING (SELECT %(recommendation_type)s AS recommendation_type) s
                    ON t.recommendation_type = s.recommendation_type
                    WHEN NOT MATCHED THEN INSERT (
                      playbook_id, recommendation_type, playbook_name, prerequisites, quick_fix, best_fix, long_term_fix, validation_sql, risk_class
                    ) VALUES (
                      %(playbook_id)s, %(recommendation_type)s, %(playbook_name)s, %(prerequisites)s, %(quick_fix)s, %(best_fix)s, %(long_term_fix)s, %(validation_sql)s, %(risk_class)s
                    )
                    """,
                    {
                        "playbook_id": str(uuid.uuid4()),
                        "recommendation_type": rec_type,
                        "playbook_name": name,
                        "prerequisites": pre,
                        "quick_fix": quick,
                        "best_fix": best,
                        "long_term_fix": long,
                        "validation_sql": vsql,
                        "risk_class": risk,
                    },
                )
                created += 1
            conn.commit()
    finally:
        conn.close()
    return created
