import os
import re
from pathlib import Path
from typing import Optional

import snowflake.connector


def _load_env_file_once():
    if os.environ.get("_COST_COPILOT_ENV_LOADED") == "1":
        return
    project_root = Path(__file__).resolve().parents[1]
    env_path = project_root / ".env"
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    os.environ["_COST_COPILOT_ENV_LOADED"] = "1"


_load_env_file_once()


def _env_non_empty(key: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(key)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _quote_identifier(identifier: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", identifier):
        raise ValueError(f"Unsafe Snowflake identifier: {identifier}")
    return f'"{identifier}"'


def get_conn():
    database = _env_non_empty("SNOWFLAKE_DATABASE")
    schema = _env_non_empty("SNOWFLAKE_SCHEMA")
    password = _env_non_empty("SNOWFLAKE_PASSWORD")
    authenticator = _env_non_empty("SNOWFLAKE_AUTHENTICATOR")
    kwargs = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    }
    if password:
        kwargs["password"] = password
    elif authenticator:
        kwargs["authenticator"] = authenticator
    else:
        raise ValueError(
            "Snowflake auth is not configured. Set SNOWFLAKE_PASSWORD or SNOWFLAKE_AUTHENTICATOR (e.g. externalbrowser)."
        )
    if database:
        kwargs["database"] = database
    if schema:
        kwargs["schema"] = schema
    return snowflake.connector.connect(**kwargs)


def ensure_database_schema(conn):
    database = _env_non_empty("SNOWFLAKE_DATABASE", "COST_COPILOT_DB")
    schema = _env_non_empty("SNOWFLAKE_SCHEMA", "COST_COPILOT")
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(database)}")
        cur.execute(f"USE DATABASE {_quote_identifier(database)}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
        cur.execute(f"USE SCHEMA {_quote_identifier(schema)}")


def run_sql_file(conn, sql_file_path: Path):
    sql_text = sql_file_path.read_text(encoding="utf-8")
    conn.execute_string(sql_text)


def ensure_copilot_bootstrap(conn):
    ensure_database_schema(conn)
    project_root = Path(__file__).resolve().parents[1]
    bootstrap_files = [
        project_root / "sql" / "01_bootstrap.sql",
        project_root / "sql" / "02_views.sql",
        project_root / "sql" / "20_v2_extensions.sql",
        project_root / "sql" / "21_v2_views.sql",
    ]
    for sql_file in bootstrap_files:
        if sql_file.exists():
            run_sql_file(conn, sql_file)
