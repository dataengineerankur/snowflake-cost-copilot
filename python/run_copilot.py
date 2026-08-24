import argparse
import sys
from pathlib import Path

import snowflake.connector

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

import ai_recommender
import anomaly_detector
import budget_guardrails
import collector
import executor
import lineage_builder
import owner_mapper
import playbook_engine
import recommendation_scorer
import recurrence
import rules
import verification
from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def print_outputs(days: int):
    conn = get_conn()
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            print("\nTop 10 heavy QUERY_TAGs:")
            cur.execute(
                """
                SELECT query_tag, warehouse_name, user_name, credits_total, elapsed_seconds, scanned_gb
                FROM COST_COPILOT.V_HEAVY_TAGS_7D
                ORDER BY credits_total DESC
                LIMIT 10
                """
            )
            for r in cur.fetchall():
                print(
                    f"- tag={r[0]} | warehouse={r[1]} | user={r[2]} | credits={r[3]:.4f} | elapsed_s={r[4]:.2f} | scanned_gb={r[5]:.2f}"
                )

            print("\nTop 10 recommendations (rules):")
            cur.execute(
                """
                SELECT rec_id, rule_name, recommendation_type, object_name, risk, est_savings
                FROM COST_COPILOT.RECOMMENDATIONS
                ORDER BY created_at DESC
                LIMIT 10
                """
            )
            for r in cur.fetchall():
                print(f"- {r[0]} | {r[1]} | {r[2]} | {r[3]} | risk={r[4]} | est_savings={r[5]:.2f}")

            print("\nTop 10 AI recommendations:")
            cur.execute(
                """
                SELECT rec_id, ai_mode, ai_summary, confidence_score
                FROM COST_COPILOT.AI_RECOMMENDATIONS
                ORDER BY created_at DESC
                LIMIT 10
                """
            )
            for r in cur.fetchall():
                print(f"- rec_id={r[0]} | mode={r[1]} | confidence={r[3]:.2f} | {r[2]}")

            print("\nTop 10 anomalies:")
            cur.execute(
                """
                SELECT anomaly_id, object_name, severity, anomaly_score, observed_value, baseline_value
                FROM COST_COPILOT.ANOMALIES
                ORDER BY detected_at DESC
                LIMIT 10
                """
            )
            for r in cur.fetchall():
                print(f"- {r[0]} | {r[1]} | {r[2]} | score={r[3]:.2f} | obs={r[4]:.4f} | base={r[5]:.4f}")
    finally:
        conn.close()

    print("\nSample SQL snippets:")
    print(
        f"""1) Top expensive query tags
SELECT *
FROM COST_COPILOT.V_HEAVY_TAGS_7D
ORDER BY credits_total DESC
LIMIT 20;"""
    )
    print(
        f"""2) Spill-heavy queries for last {days} days
SELECT query_id, query_tag, warehouse_name, spill_gb, credits_total
FROM COST_COPILOT.V_QUERY_WASTE_SIGNALS
WHERE spill_gb > 1
ORDER BY spill_gb DESC
LIMIT 50;"""
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--mode", choices=["DRY_RUN", "APPLY"], default="DRY_RUN")
    parser.add_argument("--ai", choices=["on", "off"], default="on")
    parser.add_argument("--ui", choices=["on", "off"], default="off")
    parser.add_argument("--verify", choices=["on", "off"], default="on")
    args = parser.parse_args()

    print(f"Running collector for last {args.days} days...")
    delays = collector.run_collector(days=args.days)
    print("Collector done.")
    print("Source freshness:")
    for source, ts in delays.items():
        print(f"  - {source}: {ts}")

    print("Running rules engine...")
    rec_count = rules.generate_recommendations(days=args.days)
    print(f"Rules generated {rec_count} recommendations.")

    print("Refreshing ownership, playbooks, lineage...")
    owner_count = owner_mapper.refresh_owner_mapping()
    playbook_engine.seed_playbooks()
    edge_count = lineage_builder.refresh_lineage(days=args.days)
    print(f"Owner mappings refreshed: {owner_count}, lineage edges: {edge_count}")

    print("Running anomaly detection and budget checks...")
    anomaly_count = anomaly_detector.detect_anomalies(days=max(args.days, 7))
    budget_event_count = budget_guardrails.evaluate_budgets(days=30)
    print(f"Anomalies detected: {anomaly_count}, budget events: {budget_event_count}")

    if args.ai == "on":
        print("Running AI recommender...")
        ai_count = ai_recommender.generate_ai_recommendations()
        print(f"AI generated {ai_count} recommendations.")
    else:
        print("AI recommender skipped (--ai off).")

    print(f"Running executor in {args.mode}...")
    applied = executor.apply_actions(mode=args.mode)
    print(f"Executor completed. Applied actions: {applied}")

    print("Scoring recommendations and updating recurrence memory...")
    score_count = recommendation_scorer.score_recommendations(limit=500)
    recurrence_count = recurrence.update_recurrence_memory(days=30)
    print(f"Recommendation scores written: {score_count}, recurrence updates: {recurrence_count}")

    if args.verify == "on":
        verify_count = verification.run_verification(days=3)
        print(f"Verification records written: {verify_count}")

    print_outputs(days=args.days)

    if args.ui == "on":
        print("\nUI mode requested. Run in a separate terminal:")
        print("  uvicorn python.chat_api:app --reload --port 8000")
        print("  streamlit run ui/app.py")


if __name__ == "__main__":
    main()
