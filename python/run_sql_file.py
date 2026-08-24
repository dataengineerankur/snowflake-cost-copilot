import argparse
from pathlib import Path

from sf_bootstrap import ensure_database_schema, get_conn, run_sql_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to SQL file")
    args = parser.parse_args()

    sql_path = Path(args.file).resolve()
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    conn = get_conn()
    try:
        ensure_database_schema(conn)
        run_sql_file(conn, sql_path)
    finally:
        conn.close()
    print(f"Executed SQL file: {sql_path}")


if __name__ == "__main__":
    main()
