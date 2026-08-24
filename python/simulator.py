import argparse
import json
import uuid
from typing import Dict

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def run_simulation(target_type: str, target_name: str, assumptions: Dict) -> str:
    conn = get_conn()
    simulation_id = str(uuid.uuid4())
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO COST_COPILOT.SIMULATION_RUNS (
                  simulation_id, run_name, target_type, target_name, assumptions_json, created_by
                )
                SELECT
                  %(simulation_id)s, %(run_name)s, %(target_type)s, %(target_name)s, PARSE_JSON(%(assumptions)s), CURRENT_USER()
                """,
                {
                    "simulation_id": simulation_id,
                    "run_name": f"sim-{target_type}-{target_name}",
                    "target_type": target_type,
                    "target_name": target_name,
                    "assumptions": json.dumps(assumptions),
                },
            )

            scan_reduction = float(assumptions.get("scan_reduction_pct", 0.2))
            spill_reduction = float(assumptions.get("spill_reduction_pct", 0.3))
            runtime_delta = -0.15 if assumptions.get("warehouse_resize") else -0.05
            expected_credit_delta = -(scan_reduction * 0.4 + spill_reduction * 0.2)
            expected_usd_delta = expected_credit_delta * 100.0 * 3.0
            confidence = 0.65 + min(scan_reduction, 0.3)

            cur.execute(
                """
                INSERT INTO COST_COPILOT.SIMULATION_RESULTS (
                  result_id, simulation_id, scenario_name, expected_credit_delta, expected_runtime_delta_pct,
                  expected_usd_delta, confidence_score, assumptions_json
                )
                SELECT
                  %(result_id)s, %(simulation_id)s, %(scenario_name)s, %(expected_credit_delta)s, %(expected_runtime_delta_pct)s,
                  %(expected_usd_delta)s, %(confidence_score)s, PARSE_JSON(%(assumptions)s)
                """,
                {
                    "result_id": str(uuid.uuid4()),
                    "simulation_id": simulation_id,
                    "scenario_name": "primary",
                    "expected_credit_delta": expected_credit_delta,
                    "expected_runtime_delta_pct": runtime_delta,
                    "expected_usd_delta": expected_usd_delta,
                    "confidence_score": min(confidence, 0.95),
                    "assumptions": json.dumps(assumptions),
                },
            )
            conn.commit()
    finally:
        conn.close()
    return simulation_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-type", default="WAREHOUSE")
    parser.add_argument("--target-name", required=True)
    parser.add_argument("--scan-reduction-pct", type=float, default=0.2)
    parser.add_argument("--spill-reduction-pct", type=float, default=0.3)
    parser.add_argument("--warehouse-resize", action="store_true")
    args = parser.parse_args()

    assumptions = {
        "scan_reduction_pct": args.scan_reduction_pct,
        "spill_reduction_pct": args.spill_reduction_pct,
        "warehouse_resize": args.warehouse_resize,
    }
    sim_id = run_simulation(args.target_type, args.target_name, assumptions)
    print(f"Simulation completed: {sim_id}")


if __name__ == "__main__":
    main()
