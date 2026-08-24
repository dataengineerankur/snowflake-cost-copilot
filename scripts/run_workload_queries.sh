#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  source .env
  set +a
fi

python python/run_sql_file.py --file sql/12_run_workload.sql
python python/run_copilot.py --days 7 --mode DRY_RUN --ai on
