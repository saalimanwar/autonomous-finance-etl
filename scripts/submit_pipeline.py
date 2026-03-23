"""
submit_pipeline.py
─────────────────────────────────────────────────────────────────
Submits 3 sample Finance pipeline configs to ETL_CONFIGS.
These are the pipelines CoCo will process when you run the agent.

Pipeline 1: TRANSACTIONS_RAW → TRANSACTIONS_CLEAN  (INCREMENTAL)
Pipeline 2: ACCOUNTS_RAW    → ACCOUNTS_CLEAN       (FULL_LOAD)
Pipeline 3: FRAUD_FLAGS_RAW → FRAUD_FLAGS_CLEAN    (CDC/MERGE)

HOW TO RUN:
  python scripts/submit_pipeline.py
"""

import os
import json
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_AGENT_WH"),
        database  = os.environ.get("SNOWFLAKE_DATABASE",  "ETL_AGENT_DB"),
        schema    = "ETL_META",
    )


# ── Pipeline configs ───────────────────────────────────────────────────────
PIPELINES = [

    # ── Pipeline 1: INCREMENTAL ───────────────────────────────────────────
    {
        "pipeline_id":   "ETL_TRANSACTIONS_001",
        "pipeline_name": "transactions_raw_to_clean",
        "domain":        "finance",
        "source_table":  "FINANCE_DB.RAW.TRANSACTIONS_RAW",
        "target_table":  "FINANCE_DB.CLEAN.TRANSACTIONS_CLEAN",
        "load_strategy": "INCREMENTAL",
        "params": {
            "identity": {
                "pipeline_name": "transactions_raw_to_clean",
                "domain":        "finance",
                "owner_email":   "data-team@company.com"
            },
            "source": {
                "table":          "FINANCE_DB.RAW.TRANSACTIONS_RAW",
                "filter":         "STATUS NOT IN ('TEST', 'CANCELLED')",
                "deduplicate":    False,
                "dedup_key":      None,
                "dedup_order_by": None
            },
            "target": {
                "table":      "FINANCE_DB.CLEAN.TRANSACTIONS_CLEAN",
                "write_mode": "append"
            },
            "strategy": {
                "type":               "INCREMENTAL",
                "watermark_col":      "UPDATED_AT",
                "watermark_initial":  "2024-01-01",
                "merge_keys":         [],
                "delete_flag_col":    None
            },
            "transformations": {
                "nl_rules": (
                    "1. Mask ACCOUNT_NUMBER using SHA2(ACCOUNT_NUMBER, 256) "
                    "and store in ACCOUNT_NUMBER_HASH. "
                    "2. Cast TX_DATE from VARCHAR to DATE — handle both "
                    "YYYY-MM-DD and MM/DD/YYYY formats using TRY_TO_DATE. "
                    "3. COALESCE TX_AMOUNT to 0 if NULL. "
                    "4. Parse TX_PAYLOAD JSON: extract merchant_name, "
                    "merchant_category, merchant_city, merchant_country, payment_method. "
                    "5. Set CUSTOMER_ID = SHA2(ACCOUNT_NUMBER, 256) — same hash as above."
                ),
                "column_mappings": []
            },
            "execution": {
                "warehouse":       "ETL_AGENT_WH",
                "schedule":        "1 HOUR",
                "timeout_minutes": 30,
                "max_retries":     2
            },
            "quality": {
                "error_tolerance_pct": 1.0,
                "null_check_cols":     ["TRANSACTION_ID"],
                "min_rows_expected":   0,
                "notification_email":  None
            },
            "hooks": {
                "pre_load_sql":  None,
                "post_load_sql": None
            }
        }
    },

    # ── Pipeline 2: FULL LOAD ─────────────────────────────────────────────
    {
        "pipeline_id":   "ETL_ACCOUNTS_001",
        "pipeline_name": "accounts_raw_to_clean",
        "domain":        "finance",
        "source_table":  "FINANCE_DB.RAW.ACCOUNTS_RAW",
        "target_table":  "FINANCE_DB.CLEAN.ACCOUNTS_CLEAN",
        "load_strategy": "FULL_LOAD",
        "params": {
            "identity": {
                "pipeline_name": "accounts_raw_to_clean",
                "domain":        "finance",
                "owner_email":   "data-team@company.com"
            },
            "source": {
                "table":          "FINANCE_DB.RAW.ACCOUNTS_RAW",
                "filter":         None,
                "deduplicate":    False,
                "dedup_key":      None,
                "dedup_order_by": None
            },
            "target": {
                "table":      "FINANCE_DB.CLEAN.ACCOUNTS_CLEAN",
                "write_mode": "overwrite"
            },
            "strategy": {
                "type":              "FULL_LOAD",
                "watermark_col":     None,
                "watermark_initial": None,
                "merge_keys":        [],
                "delete_flag_col":   None
            },
            "transformations": {
                "nl_rules": (
                    "1. Cast BALANCE from VARCHAR (e.g. '12,450.75') to NUMBER "
                    "using REPLACE(BALANCE, ',', '')::NUMBER(18,2). "
                    "2. Cast CREDIT_LIMIT same way — NULL for non-credit accounts. "
                    "3. Strip % from INTEREST_RATE and cast: "
                    "REPLACE(INTEREST_RATE,'%','')::NUMBER(8,4). "
                    "4. Cast RISK_SCORE from VARCHAR to NUMBER(4,0). "
                    "5. Decode ACCOUNT_STATUS: A=Active, S=Suspended, C=Closed, D=Dormant. "
                    "6. Derive RISK_BAND: 1-3=LOW, 4-5=MEDIUM, 6-7=HIGH, 8-10=CRITICAL. "
                    "7. Cast OPENED_DATE from DD-MON-YYYY using TO_DATE(OPENED_DATE, 'DD-MON-YYYY'). "
                    "8. Cast LAST_ACTIVITY_DATE same way. "
                    "9. DROP RELATIONSHIP_MGR — PII not needed in clean layer."
                ),
                "column_mappings": []
            },
            "execution": {
                "warehouse":       "ETL_AGENT_WH",
                "schedule":        "24 HOUR",
                "timeout_minutes": 60,
                "max_retries":     1
            },
            "quality": {
                "error_tolerance_pct": 0.5,
                "null_check_cols":     ["ACCOUNT_ID", "CUSTOMER_ID"],
                "min_rows_expected":   5,
                "notification_email":  None
            },
            "hooks": {
                "pre_load_sql":  None,
                "post_load_sql": None
            }
        }
    },

    # ── Pipeline 3: CDC / MERGE ───────────────────────────────────────────
    {
        "pipeline_id":   "ETL_FRAUD_FLAGS_001",
        "pipeline_name": "fraud_flags_raw_to_clean",
        "domain":        "finance",
        "source_table":  "FINANCE_DB.RAW.FRAUD_FLAGS_RAW",
        "target_table":  "FINANCE_DB.CLEAN.FRAUD_FLAGS_CLEAN",
        "load_strategy": "CDC",
        "params": {
            "identity": {
                "pipeline_name": "fraud_flags_raw_to_clean",
                "domain":        "finance",
                "owner_email":   "fraud-team@company.com"
            },
            "source": {
                "table":          "FINANCE_DB.RAW.FRAUD_FLAGS_RAW",
                "filter":         None,
                "deduplicate":    True,
                "dedup_key":      "TRANSACTION_ID",
                "dedup_order_by": "UPDATED_AT DESC"
            },
            "target": {
                "table":      "FINANCE_DB.CLEAN.FRAUD_FLAGS_CLEAN",
                "write_mode": "upsert"
            },
            "strategy": {
                "type":              "CDC",
                "watermark_col":     None,
                "watermark_initial": None,
                "merge_keys":        ["TRANSACTION_ID"],
                "delete_flag_col":   "IS_DELETED"
            },
            "transformations": {
                "nl_rules": (
                    "1. Cast FRAUD_SCORE from VARCHAR to NUMBER(5,2). "
                    "2. Cast SEVERITY_CODE from VARCHAR to NUMBER(2,0). "
                    "3. Derive SEVERITY_LABEL: 1=LOW, 2=MEDIUM, 3=HIGH, 4=CRITICAL, 5=BLOCKED. "
                    "4. Convert DETECTION_TIMESTAMP_MS from epoch milliseconds to TIMESTAMP_NTZ: "
                    "TO_TIMESTAMP_NTZ(DETECTION_TIMESTAMP_MS / 1000). "
                    "5. Derive RULE_COUNT: ARRAY_SIZE(SPLIT(RULES_TRIGGERED, ',')). "
                    "6. Cast ANALYST_REVIEWED from VARCHAR 'TRUE'/'FALSE' to BOOLEAN: "
                    "CASE WHEN ANALYST_REVIEWED = 'TRUE' THEN TRUE ELSE FALSE END. "
                    "7. IS_DELETED rows should trigger DELETE in the MERGE."
                ),
                "column_mappings": []
            },
            "execution": {
                "warehouse":       "ETL_AGENT_WH",
                "schedule":        "15 MINUTE",
                "timeout_minutes": 15,
                "max_retries":     3
            },
            "quality": {
                "error_tolerance_pct": 2.0,
                "null_check_cols":     ["TRANSACTION_ID"],
                "min_rows_expected":   0,
                "notification_email":  None
            },
            "hooks": {
                "pre_load_sql":  None,
                "post_load_sql": None
            }
        }
    },
]


def main():
    print("Connecting to Snowflake...")
    conn = get_connection()
    cur  = conn.cursor()
    print("  ✓ Connected\n")

    inserted = 0
    skipped  = 0

    for p in PIPELINES:
        pid = p["pipeline_id"]

        # Check if already exists
        cur.execute(
            "SELECT COUNT(*) FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS WHERE PIPELINE_ID = %s",
            (pid,)
        )
        exists = cur.fetchone()[0]

        if exists:
            print(f"  ⚠ Skipping {pid} — already exists")
            skipped += 1
            continue

        params_json = json.dumps(p["params"])

        cur.execute("""
            INSERT INTO ETL_AGENT_DB.ETL_META.ETL_CONFIGS (
                PIPELINE_ID, PIPELINE_NAME, DOMAIN,
                SOURCE_TABLE, TARGET_TABLE, LOAD_STRATEGY,
                PIPELINE_PARAMS, STATUS, CREATED_BY
            )
            SELECT
                %s, %s, %s, %s, %s, %s,
                PARSE_JSON(%s),
                'PENDING',
                CURRENT_USER()
        """, (
            pid,
            p["pipeline_name"],
            p["domain"],
            p["source_table"],
            p["target_table"],
            p["load_strategy"],
            params_json,
        ))

        print(f"  ✓ Submitted: {pid} ({p['load_strategy']}) — {p['pipeline_name']}")
        inserted += 1

    print(f"\n{'─'*50}")
    print(f"✅ Done — {inserted} pipelines submitted, {skipped} skipped (already exist)")
    print()
    print("Next step — open Snowsight → CoCo → paste SKILL.md")
    print("CoCo will read the queue and start processing pipelines.")
    print()
    print("Or check the queue manually:")
    print("  SELECT * FROM ETL_AGENT_DB.ETL_META.V_PENDING_PIPELINES;")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
