import argparse
import json
import uuid

from event_bus import emit_event
from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def evaluate_budgets(days: int = 30) -> int:
    conn = get_conn()
    created = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                MERGE INTO COST_COPILOT.BUDGETS t
                USING (
                  SELECT 'default-query-tag' AS budget_id, 'Default Query Tag Budget' AS budget_name, 'QUERY_TAG' AS scope_type,
                         'UNTAGGED' AS scope_value, 'MONTHLY' AS period, 50.0 AS budget_credits, 150.0 AS budget_usd
                ) s
                ON t.budget_id = s.budget_id
                WHEN NOT MATCHED THEN INSERT (budget_id, budget_name, scope_type, scope_value, period, budget_credits, budget_usd)
                VALUES (s.budget_id, s.budget_name, s.scope_type, s.scope_value, s.period, s.budget_credits, s.budget_usd)
                """
            )

            cur.execute(
                """
                SELECT budget_id, scope_type, scope_value, budget_credits, budget_usd, warning_threshold_pct, critical_threshold_pct
                FROM COST_COPILOT.BUDGETS
                WHERE active = TRUE
                """
            )
            budgets = cur.fetchall()

            for budget in budgets:
                (
                    budget_id,
                    scope_type,
                    scope_value,
                    budget_credits,
                    budget_usd,
                    warn_pct,
                    crit_pct,
                ) = budget
                observed_credits = 0.0
                if scope_type == "QUERY_TAG":
                    cur.execute(
                        """
                        SELECT COALESCE(SUM(c.credits_attributed_compute + c.credits_used_query_acceleration), 0)
                        FROM COST_COPILOT.FACT_QUERY_COST c
                        WHERE COALESCE(c.query_tag, 'UNTAGGED') = %(scope_value)s
                          AND c.start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                        """,
                        {"scope_value": scope_value, "days": days},
                    )
                    observed_credits = float(cur.fetchone()[0] or 0)
                elif scope_type == "WAREHOUSE":
                    cur.execute(
                        """
                        SELECT COALESCE(SUM(credits_used), 0)
                        FROM COST_COPILOT.FACT_WAREHOUSE_HOURLY
                        WHERE warehouse_name = %(scope_value)s
                          AND start_time >= DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP())
                        """,
                        {"scope_value": scope_value, "days": days},
                    )
                    observed_credits = float(cur.fetchone()[0] or 0)

                observed_usd = observed_credits * 3.0
                budget_threshold = max(float(budget_credits or 1.0), 0.001)
                burn_rate = observed_credits / budget_threshold
                days_to_exhaustion = (days / burn_rate) if burn_rate > 0 else None

                threshold_level = None
                if burn_rate >= float(crit_pct or 1.0):
                    threshold_level = "CRITICAL"
                elif burn_rate >= float(warn_pct or 0.8):
                    threshold_level = "WARNING"

                if threshold_level:
                    event_id = str(uuid.uuid4())
                    evidence = {
                        "scope_type": scope_type,
                        "scope_value": scope_value,
                        "budget_credits": float(budget_credits or 0),
                        "observed_credits": observed_credits,
                        "days": days,
                    }
                    cur.execute(
                        """
                        INSERT INTO COST_COPILOT.BUDGET_EVENTS (
                          event_id, budget_id, window_start, window_end, observed_credits, observed_usd, burn_rate,
                          days_to_exhaustion, threshold_level, evidence_json
                        )
                        SELECT
                          %(event_id)s, %(budget_id)s, DATEADD(DAY, -%(days)s, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(),
                          %(observed_credits)s, %(observed_usd)s, %(burn_rate)s, %(days_to_exhaustion)s, %(threshold_level)s,
                          PARSE_JSON(%(evidence)s)
                        """,
                        {
                            "event_id": event_id,
                            "budget_id": budget_id,
                            "days": days,
                            "observed_credits": observed_credits,
                            "observed_usd": observed_usd,
                            "burn_rate": burn_rate,
                            "days_to_exhaustion": days_to_exhaustion,
                            "threshold_level": threshold_level,
                            "evidence": json.dumps(evidence),
                        },
                    )
                    emit_event(
                        cur,
                        "budget.breached",
                        {
                            "event_id": event_id,
                            "budget_id": budget_id,
                            "scope_value": scope_value,
                            "burn_rate": burn_rate,
                            "threshold_level": threshold_level,
                        },
                    )
                    created += 1
            conn.commit()
    finally:
        conn.close()
    return created


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    count = evaluate_budgets(days=args.days)
    print(f"Created {count} budget events.")


if __name__ == "__main__":
    main()
