import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON_DIR = ROOT / "python"
if str(PYTHON_DIR) not in sys.path:
    sys.path.append(str(PYTHON_DIR))

from owner_mapper import parse_tag  # noqa: E402


def test_parse_dbt_tag():
    owner_id, source = parse_tag("dbt:prod:model=fct_orders:run=abc")
    assert source == "dbt"
    assert owner_id.startswith("owner-dbt")


def test_parse_unknown_tag():
    owner_id, source = parse_tag("random-tag")
    assert owner_id == "owner-unowned"
    assert source == "UNOWNED"
