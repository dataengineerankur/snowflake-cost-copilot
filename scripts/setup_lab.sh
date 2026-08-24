#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  source .env
  set +a
fi

echo "[1/4] Terraform apply for base Snowflake lab objects..."
cd "$ROOT_DIR/infra/snowflake_lab"

if [[ -z "${SNOWFLAKE_ACCOUNT:-}" || -z "${SNOWFLAKE_USER:-}" || -z "${SNOWFLAKE_ROLE:-}" ]]; then
  echo "Missing required Snowflake env vars. Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE in .env"
  exit 1
fi

RAW_AUTH="${SNOWFLAKE_AUTHENTICATOR:-Snowflake}"
RAW_AUTH_LC="$(printf '%s' "$RAW_AUTH" | tr '[:upper:]' '[:lower:]')"
case "${RAW_AUTH_LC}" in
  snowflake) SNOWFLAKE_AUTHENTICATOR="Snowflake" ;;
  externalbrowser) SNOWFLAKE_AUTHENTICATOR="ExternalBrowser" ;;
  oauth) SNOWFLAKE_AUTHENTICATOR="OAuth" ;;
  okta) SNOWFLAKE_AUTHENTICATOR="Okta" ;;
  jwt) SNOWFLAKE_AUTHENTICATOR="JWT" ;;
  tokenaccessor) SNOWFLAKE_AUTHENTICATOR="TokenAccessor" ;;
  usernamepasswordmfa) SNOWFLAKE_AUTHENTICATOR="UsernamePasswordMFA" ;;
  *)
    echo "Invalid SNOWFLAKE_AUTHENTICATOR='${RAW_AUTH}'. Use Snowflake or ExternalBrowser."
    exit 1
    ;;
esac
SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-}"
if [[ "$SNOWFLAKE_AUTHENTICATOR" == "Snowflake" && -z "$SNOWFLAKE_PASSWORD" ]]; then
  echo "SNOWFLAKE_PASSWORD is empty while SNOWFLAKE_AUTHENTICATOR=snowflake. Set password or use externalbrowser."
  exit 1
fi

cat > terraform.tfvars <<EOF
snowflake_account       = "${SNOWFLAKE_ACCOUNT}"
snowflake_user          = "${SNOWFLAKE_USER}"
snowflake_password      = "${SNOWFLAKE_PASSWORD}"
snowflake_authenticator = "${SNOWFLAKE_AUTHENTICATOR}"
snowflake_role          = "${SNOWFLAKE_ROLE}"
snowflake_bootstrap_warehouse = "${SNOWFLAKE_BOOTSTRAP_WAREHOUSE:-COMPUTE_WH}"

database_name           = "${SNOWFLAKE_DATABASE:-COST_COPILOT_DB}"
warehouse_name          = "${SNOWFLAKE_WAREHOUSE:-COST_COPILOT_LAB_WH}"
warehouse_size          = "MEDIUM"
EOF

if [[ "$SNOWFLAKE_AUTHENTICATOR" == "ExternalBrowser" ]]; then
  echo "Using external browser authentication for Terraform provider."
fi

terraform init
terraform apply -auto-approve

echo "[2/4] Creating streams, tasks, procedures, complex view..."
cd "$ROOT_DIR"
python python/run_sql_file.py --file sql/10_lab_objects.sql

echo "[3/4] Seeding 10M+ records..."
python python/run_sql_file.py --file sql/11_seed_lab_data.sql

echo "[4/4] Running heavy tagged workload queries..."
python python/run_sql_file.py --file sql/12_run_workload.sql

echo "Lab setup complete."
