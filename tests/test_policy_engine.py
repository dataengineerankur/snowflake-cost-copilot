import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON_DIR = ROOT / "python"
if str(PYTHON_DIR) not in sys.path:
    sys.path.append(str(PYTHON_DIR))

from policy_engine import evaluate_policy  # noqa: E402


def test_policy_allows_low_risk_approved_allowlisted():
    rec = {"rec_id": "r1", "risk": "LOW", "object_name": "DEV_WH"}
    approvals = [{"rec_id": "r1", "approved": True, "approved_by": "alice"}]
    allowed, reason = evaluate_policy(rec, approvals, ["DEV_WH"])
    assert allowed is True
    assert reason == "OK"


def test_policy_blocks_non_low_risk():
    rec = {"rec_id": "r1", "risk": "HIGH", "object_name": "DEV_WH"}
    approvals = [{"rec_id": "r1", "approved": True, "approved_by": "alice"}]
    allowed, reason = evaluate_policy(rec, approvals, ["DEV_WH"])
    assert allowed is False
    assert reason == "RISK_NOT_LOW"
