import argparse
import json
import uuid

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def refresh_lineage(days: int = 7) -> int:
    conn = get_conn()
    inserted = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM COST_COPILOT.FACT_LINEAGE_EDGES WHERE created_at >= DATEADD(DAY, -1, CURRENT_TIMESTAMP())")
            cur.execute(
                """
                SELECT
                  query_id,
                  COALESCE(query_tag, 'UNTAGGED') AS query_tag,
                  COALESCE(warehouse_name, 'UNKNOWN') AS warehouse_name,
                  COALESCE(user_name, 'UNKNOWN') AS user_name,
                  COALESCE(role_name, 'UNKNOWN') AS role_name
                FROM COST_COPILOT.FACT_QUERY
                WHERE start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                """,
                {"days": days},
            )
            for query_id, query_tag, warehouse_name, user_name, role_name in cur.fetchall():
                edges = [
                    ("QUERY", query_id, "QUERY_TAG", query_tag, "ATTRIBUTED_TO"),
                    ("QUERY", query_id, "WAREHOUSE", warehouse_name, "EXECUTED_ON"),
                    ("QUERY", query_id, "USER", user_name, "EXECUTED_BY"),
                    ("QUERY", query_id, "ROLE", role_name, "ASSUMED_ROLE"),
                ]
                for src_type, src_name, dst_type, dst_name, rel in edges:
                    cur.execute(
                        """
                        INSERT INTO COST_COPILOT.FACT_LINEAGE_EDGES (
                          edge_id, src_type, src_name, dst_type, dst_name, relation_type, weight, evidence_json
                        )
                        SELECT
                          %(edge_id)s, %(src_type)s, %(src_name)s, %(dst_type)s, %(dst_name)s, %(relation_type)s, 1.0,
                          PARSE_JSON(%(evidence)s)
                        """,
                        {
                            "edge_id": str(uuid.uuid4()),
                            "src_type": src_type,
                            "src_name": src_name,
                            "dst_type": dst_type,
                            "dst_name": dst_name,
                            "relation_type": rel,
                            "evidence": json.dumps({"query_id": query_id}),
                        },
                    )
                    inserted += 1
            conn.commit()
    finally:
        conn.close()
    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    count = refresh_lineage(days=args.days)
    print(f"Inserted {count} lineage edges.")


if __name__ == "__main__":
    main()
