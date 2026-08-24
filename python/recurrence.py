import argparse
import json
import uuid

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def update_recurrence_memory(days: int = 30) -> int:
    conn = get_conn()
    changed = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT recommendation_type, object_type, object_name, COUNT(*) AS cnt, MIN(created_at), MAX(created_at)
                FROM COST_COPILOT.RECOMMENDATIONS
                WHERE created_at >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                GROUP BY 1,2,3
                HAVING COUNT(*) >= 2
                ORDER BY cnt DESC
                """,
                {"days": days},
            )
            for recommendation_type, object_type, object_name, cnt, first_seen, last_seen in cur.fetchall():
                fingerprint = f"{recommendation_type}:{object_type}:{object_name}"
                cur.execute(
                    """
                    MERGE INTO COST_COPILOT.RECURRENCE_MEMORY t
                    USING (SELECT %(fingerprint)s AS fingerprint) s
                    ON t.fingerprint = s.fingerprint
                    WHEN MATCHED THEN UPDATE SET
                      last_seen = %(last_seen)s,
                      occurrence_count = %(occurrence_count)s,
                      severity = IFF(%(occurrence_count)s >= 5, 'HIGH', 'MEDIUM'),
                      context_json = PARSE_JSON(%(context)s)
                    WHEN NOT MATCHED THEN INSERT (
                      memory_id, fingerprint, object_type, object_name, first_seen, last_seen,
                      occurrence_count, last_verdict, severity, context_json
                    ) VALUES (
                      %(memory_id)s, %(fingerprint)s, %(object_type)s, %(object_name)s, %(first_seen)s, %(last_seen)s,
                      %(occurrence_count)s, 'OPEN', IFF(%(occurrence_count)s >= 5, 'HIGH', 'MEDIUM'), PARSE_JSON(%(context)s)
                    )
                    """,
                    {
                        "memory_id": str(uuid.uuid4()),
                        "fingerprint": fingerprint,
                        "object_type": object_type,
                        "object_name": object_name,
                        "first_seen": first_seen,
                        "last_seen": last_seen,
                        "occurrence_count": int(cnt or 0),
                        "context": json.dumps({"recommendation_type": recommendation_type}),
                    },
                )
                changed += 1
            conn.commit()
    finally:
        conn.close()
    return changed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    count = update_recurrence_memory(days=args.days)
    print(f"Recurrence records upserted: {count}")


if __name__ == "__main__":
    main()
