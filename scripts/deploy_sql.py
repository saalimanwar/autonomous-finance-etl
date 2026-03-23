"""
deploy_sql.py
─────────────────────────────────────────────────────────────────
Runs all SQL files against Snowflake in order.
Handles stored procedures with $$ delimiters correctly.

HOW TO RUN:
  python scripts/deploy_sql.py
"""

import os
import sys
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SQL_DIR = Path(__file__).parent.parent / "sql"

SQL_FILES_IN_ORDER = [
    "01_setup.sql",
    "02_mock_data.sql",
    "03_tasks.sql",
]


def get_connection():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_AGENT_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE",  "ETL_AGENT_DB"),
    )


def split_sql_statements(sql_text):
    """
    Split SQL into statements correctly.
    Does NOT split on semicolons inside $$ ... $$ blocks.
    """
    statements = []
    current = []
    inside_dollar_block = False

    for line in sql_text.splitlines():
        stripped = line.strip()

        # Skip pure comment lines and empty lines
        if not stripped or stripped.startswith("--"):
            continue

        # Toggle $$ block tracking
        dollar_count = stripped.count("$$")
        if dollar_count % 2 == 1:
            inside_dollar_block = not inside_dollar_block

        current.append(line)

        # Only split on semicolons OUTSIDE $$ blocks
        if not inside_dollar_block and stripped.endswith(";"):
            full_stmt = "\n".join(current).strip()
            if full_stmt and full_stmt != ";":
                statements.append(full_stmt)
            current = []

    remaining = "\n".join(current).strip()
    if remaining and remaining != ";":
        statements.append(remaining)

    return statements


def run_sql_file(cur, filepath):
    sql = filepath.read_text(encoding="utf-8")
    statements = split_sql_statements(sql)
    count = 0
    errors = 0

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            cur.execute(stmt)
            count += 1
        except snowflake.connector.errors.ProgrammingError as e:
            if "already exists" in str(e).lower():
                count += 1
            else:
                print(f"    ⚠ {e}")
                errors += 1

    return count, errors


def main():
    filter_name = sys.argv[1] if len(sys.argv) > 1 else None
    files_to_run = [f for f in SQL_FILES_IN_ORDER
                    if (filter_name is None or filter_name in f)]

    if not files_to_run:
        print(f"No files matched: {filter_name}")
        return

    print("Connecting to Snowflake...")
    conn = get_connection()
    cur  = conn.cursor()
    print("  ✓ Connected\n")

    total_stmts  = 0
    total_errors = 0

    for fname in files_to_run:
        fpath = SQL_DIR / fname
        if not fpath.exists():
            print(f"  ⚠ File not found: {fpath} — skipping")
            continue

        print(f"Running {fname}...")
        n, e = run_sql_file(cur, fpath)
        total_stmts  += n
        total_errors += e
        status = "[OK]" if e == 0 else "[WARN]"
        print(f"  {status} {n} statements executed"
              + (f"  ({e} warnings)" if e > 0 else "") + "\n")

    print("─" * 50)
    if total_errors == 0:
        print(f"✅ Done — {total_stmts} statements, zero errors.")
    else:
        print(f"⚠ Done — {total_stmts} statements, {total_errors} errors above.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
