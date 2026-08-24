import argparse
import json
import uuid

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def score_recommendations(limit: int = 200) -> int:
    conn = get_conn()
    scored = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.rec_id, r.evidence_json, r.risk, r.recommendation_type
                FROM COST_COPILOT.RECOMMENDATIONS r
                LEFT JOIN COST_COPILOT.RECOMMENDATION_SCORES s ON r.rec_id = s.rec_id
                WHERE s.rec_id IS NULL
                ORDER BY r.created_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            for rec_id, evidence, risk, recommendation_type in cur.fetchall():
                if isinstance(evidence, str):
                    evidence = json.loads(evidence)
                evidence = evidence or {}
                fields = ["window_days", "top_query_ids", "query_tag", "warehouse_name"]
                present = sum(1 for f in fields if evidence.get(f))
                completeness = present / len(fields)
                consistency = 0.9 if recommendation_type else 0.6
                calibration = 0.9 if risk in ("LOW", "MEDIUM", "HIGH") else 0.5
                final_score = round((completeness * 0.4 + consistency * 0.3 + calibration * 0.3), 4)
                suppression_reason = None
                if final_score < 0.45:
                    suppression_reason = "LOW_QUALITY"

                cur.execute(
                    """
                    INSERT INTO COST_COPILOT.RECOMMENDATION_SCORES (
                      score_id, rec_id, evidence_completeness_score, consistency_score,
                      confidence_calibration_score, final_quality_score, suppression_reason
                    )
                    VALUES (
                      %(score_id)s, %(rec_id)s, %(ecs)s, %(consistency)s, %(calibration)s,
                      %(final_score)s, %(suppression_reason)s
                    )
                    """,
                    {
                        "score_id": str(uuid.uuid4()),
                        "rec_id": rec_id,
                        "ecs": completeness,
                        "consistency": consistency,
                        "calibration": calibration,
                        "final_score": final_score,
                        "suppression_reason": suppression_reason,
                    },
                )
                scored += 1
            conn.commit()
    finally:
        conn.close()
    return scored


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()
    count = score_recommendations(limit=args.limit)
    print(f"Scored {count} recommendations.")


if __name__ == "__main__":
    main()
