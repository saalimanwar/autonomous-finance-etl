-- =============================================================================
-- ETL AGENT — setup.sql
-- Complete Snowflake infrastructure for the domain-agnostic autonomous ETL agent.
-- Run this ONCE after mock_data.sql, before running the Streamlit app.
-- Replace all <PLACEHOLDERS> with your actual values before executing.
-- =============================================================================


-- =============================================================================
-- SECTION 1: DATABASE + SCHEMA
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ETL_AGENT_DB;

CREATE SCHEMA IF NOT EXISTS ETL_AGENT_DB.ETL_META
    COMMENT = 'ETL Agent metadata: configs, watermarks, run logs, procedures';

USE DATABASE ETL_AGENT_DB;
USE SCHEMA   ETL_META;


-- =============================================================================
-- SECTION 2: WAREHOUSES
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS ETL_AGENT_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ETL Agent — metadata queries, task management, task execution';


-- =============================================================================
-- SECTION 3: ROLES + USERS
-- =============================================================================

CREATE ROLE IF NOT EXISTS ETL_AGENT_ROLE
    COMMENT = 'Cortex CLI agent — reads configs, generates SQL, deploys tasks';

CREATE ROLE IF NOT EXISTS ETL_UI_ROLE
    COMMENT = 'Streamlit UI — submits and monitors pipeline configs';

GRANT ROLE ETL_AGENT_ROLE TO ROLE SYSADMIN;
GRANT ROLE ETL_UI_ROLE    TO ROLE SYSADMIN;

-- Replace <AGENT_USER> and <UI_USER> with real Snowflake usernames
GRANT ROLE ETL_AGENT_ROLE TO USER ANWAR;
GRANT ROLE ETL_UI_ROLE    TO USER ANWAR;


-- =============================================================================
-- SECTION 4: WAREHOUSE GRANTS
-- =============================================================================

GRANT USAGE ON WAREHOUSE ETL_AGENT_WH TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON WAREHOUSE ETL_AGENT_WH TO ROLE ETL_UI_ROLE;


-- =============================================================================
-- SECTION 5: DATABASE + SCHEMA GRANTS
-- =============================================================================

GRANT USAGE ON DATABASE ETL_AGENT_DB          TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON DATABASE ETL_AGENT_DB          TO ROLE ETL_UI_ROLE;
GRANT USAGE ON SCHEMA ETL_AGENT_DB.ETL_META   TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON SCHEMA ETL_AGENT_DB.ETL_META   TO ROLE ETL_UI_ROLE;

GRANT CREATE TABLE     ON SCHEMA ETL_AGENT_DB.ETL_META TO ROLE ETL_AGENT_ROLE;
GRANT CREATE VIEW      ON SCHEMA ETL_AGENT_DB.ETL_META TO ROLE ETL_AGENT_ROLE;
GRANT CREATE TASK      ON SCHEMA ETL_AGENT_DB.ETL_META TO ROLE ETL_AGENT_ROLE;
GRANT CREATE PROCEDURE ON SCHEMA ETL_AGENT_DB.ETL_META TO ROLE ETL_AGENT_ROLE;
GRANT CREATE TABLE     ON SCHEMA ETL_AGENT_DB.ETL_META TO ROLE ETL_UI_ROLE;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE ETL_AGENT_ROLE;


-- =============================================================================
-- SECTION 6: CORE TABLE — ETL_CONFIGS
--
-- Design principle: domain-agnostic via PIPELINE_PARAMS VARIANT column.
-- Fixed columns = only what every pipeline shares regardless of domain.
-- Everything else (source filter, column mappings, quality thresholds,
-- hooks, execution params) lives in PIPELINE_PARAMS as JSON.
-- This means a Finance pipeline and a Healthcare pipeline next month
-- store completely different fields in the same table with zero schema changes.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ETL_AGENT_DB.ETL_META.ETL_CONFIGS (

    -- Identity (fixed — every pipeline has these)
    PIPELINE_ID         VARCHAR(64)     NOT NULL,
    PIPELINE_NAME       VARCHAR(256)    NOT NULL,
    DOMAIN              VARCHAR(64),

    -- Full config JSON (domain-agnostic, flexible)
    PIPELINE_PARAMS     VARIANT         NOT NULL,

    -- Denormalized fast-access columns (mirrored from JSON, set by UI)
    SOURCE_TABLE        VARCHAR(512),
    TARGET_TABLE        VARCHAR(512),
    LOAD_STRATEGY       VARCHAR(32),

    -- Agent outputs
    GENERATED_SQL       TEXT,
    TASK_NAME           VARCHAR(256),

    -- State machine
    -- PENDING -> APPROVED -> DEPLOYED
    -- PENDING -> REJECTED
    -- APPROVED -> FAILED
    STATUS              VARCHAR(32)     NOT NULL DEFAULT 'PENDING',

    -- Audit
    CREATED_BY          VARCHAR(256),
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    APPROVED_BY         VARCHAR(256),
    APPROVED_AT         TIMESTAMP_NTZ,
    DEPLOYED_AT         TIMESTAMP_NTZ,
    ERROR_MESSAGE       TEXT,

    -- Primary Key is accepted by Snowflake (though not enforced)
    CONSTRAINT PK_ETL_CONFIGS PRIMARY KEY (PIPELINE_ID)
);

GRANT SELECT, INSERT, UPDATE
    ON TABLE ETL_AGENT_DB.ETL_META.ETL_CONFIGS TO ROLE ETL_AGENT_ROLE;
GRANT SELECT, INSERT
    ON TABLE ETL_AGENT_DB.ETL_META.ETL_CONFIGS TO ROLE ETL_UI_ROLE;


-- =============================================================================
-- SECTION 7: WATERMARK TRACKING TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS ETL_AGENT_DB.ETL_META.ETL_WATERMARKS (
    PIPELINE_ID         VARCHAR(64)     NOT NULL,
    WATERMARK_COL       VARCHAR(128)    NOT NULL,
    LAST_VALUE          VARCHAR(512),
    LAST_VALUE_TS       TIMESTAMP_NTZ,
    LAST_VALUE_NUM      NUMBER(38, 10),
    LAST_RUN_AT         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    LAST_RUN_STATUS     VARCHAR(32)     DEFAULT 'INIT',
    ROWS_LOADED         NUMBER(18, 0)   DEFAULT 0,
    UPDATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ETL_WATERMARKS PRIMARY KEY (PIPELINE_ID)
);

GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLE ETL_AGENT_DB.ETL_META.ETL_WATERMARKS TO ROLE ETL_AGENT_ROLE;


-- =============================================================================
-- SECTION 8: TASK RUN LOG TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG (
    RUN_ID                  VARCHAR(128)    NOT NULL,
    PIPELINE_ID             VARCHAR(64)     NOT NULL,
    TASK_NAME               VARCHAR(256)    NOT NULL,
    LOAD_STRATEGY           VARCHAR(32),
    RUN_START_AT            TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    RUN_END_AT              TIMESTAMP_NTZ,
    STATUS                  VARCHAR(32)     DEFAULT 'RUNNING',
    ROWS_PROCESSED          NUMBER(18, 0)   DEFAULT 0,
    WATERMARK_FROM          VARCHAR(512),
    WATERMARK_TO            VARCHAR(512),
    PRE_HOOK_STATUS         VARCHAR(32),
    POST_HOOK_STATUS        VARCHAR(32),
    QUALITY_CHECK_STATUS    VARCHAR(32),
    QUALITY_CHECK_DETAIL    TEXT,
    ERROR_MESSAGE           TEXT,
    QUERY_ID                VARCHAR(256),
    CONSTRAINT PK_ETL_TASK_RUN_LOG PRIMARY KEY (RUN_ID)
);

GRANT SELECT, INSERT, UPDATE
    ON TABLE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG TO ROLE ETL_AGENT_ROLE;
GRANT SELECT
    ON TABLE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG TO ROLE ETL_UI_ROLE;


-- =============================================================================
-- SECTION 9: VIEWS
-- =============================================================================

-- 9a. Pending queue (FIFO — agent polls this)
CREATE OR REPLACE VIEW ETL_AGENT_DB.ETL_META.V_PENDING_PIPELINES AS
SELECT
    PIPELINE_ID, PIPELINE_NAME, DOMAIN,
    SOURCE_TABLE, TARGET_TABLE, LOAD_STRATEGY,
    PIPELINE_PARAMS, CREATED_BY, CREATED_AT
FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS
WHERE STATUS = 'PENDING'
ORDER BY CREATED_AT ASC;

-- 9b. Deployed pipelines
CREATE OR REPLACE VIEW ETL_AGENT_DB.ETL_META.V_DEPLOYED_PIPELINES AS
SELECT
    PIPELINE_ID, PIPELINE_NAME, DOMAIN,
    SOURCE_TABLE, TARGET_TABLE, LOAD_STRATEGY, TASK_NAME,
    PIPELINE_PARAMS:execution:schedule::VARCHAR         AS SCHEDULE,
    PIPELINE_PARAMS:quality:notification_email::VARCHAR AS ALERT_EMAIL,
    APPROVED_BY, APPROVED_AT, DEPLOYED_AT,
    DATEDIFF('hour', DEPLOYED_AT, CURRENT_TIMESTAMP())  AS HOURS_SINCE_DEPLOY
FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS
WHERE STATUS = 'DEPLOYED'
ORDER BY DEPLOYED_AT DESC;

-- 9c. Dashboard summary
CREATE OR REPLACE VIEW ETL_AGENT_DB.ETL_META.V_PIPELINE_SUMMARY AS
SELECT
    STATUS, DOMAIN, LOAD_STRATEGY,
    COUNT(*)         AS PIPELINE_COUNT,
    MIN(CREATED_AT)  AS OLDEST,
    MAX(CREATED_AT)  AS NEWEST
FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS
GROUP BY STATUS, DOMAIN, LOAD_STRATEGY
ORDER BY STATUS, DOMAIN;

-- 9d. Last run per pipeline
CREATE OR REPLACE VIEW ETL_AGENT_DB.ETL_META.V_LAST_RUN_PER_PIPELINE AS
SELECT
    L.PIPELINE_ID,
    C.PIPELINE_NAME, C.DOMAIN, C.LOAD_STRATEGY,
    L.TASK_NAME, L.RUN_START_AT, L.RUN_END_AT,
    L.STATUS                AS LAST_RUN_STATUS,
    L.ROWS_PROCESSED,
    L.WATERMARK_FROM, L.WATERMARK_TO,
    L.QUALITY_CHECK_STATUS, L.QUALITY_CHECK_DETAIL,
    L.ERROR_MESSAGE,
    DATEDIFF('second', L.RUN_START_AT, L.RUN_END_AT) AS DURATION_SECONDS
FROM ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG L
JOIN ETL_AGENT_DB.ETL_META.ETL_CONFIGS C ON C.PIPELINE_ID = L.PIPELINE_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY L.PIPELINE_ID ORDER BY L.RUN_START_AT DESC) = 1;

-- 9e. Audit trail
CREATE OR REPLACE VIEW ETL_AGENT_DB.ETL_META.V_AUDIT_TRAIL AS
SELECT
    PIPELINE_ID, PIPELINE_NAME, DOMAIN,
    SOURCE_TABLE, TARGET_TABLE, LOAD_STRATEGY, STATUS,
    CREATED_BY, CREATED_AT, APPROVED_BY, APPROVED_AT, DEPLOYED_AT,
    DATEDIFF('minute', CREATED_AT,  APPROVED_AT) AS MINS_TO_APPROVAL,
    DATEDIFF('minute', APPROVED_AT, DEPLOYED_AT) AS MINS_TO_DEPLOY,
    ERROR_MESSAGE, TASK_NAME
FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS
ORDER BY CREATED_AT DESC;

-- 9f. Watermark health
CREATE OR REPLACE VIEW ETL_AGENT_DB.ETL_META.V_WATERMARK_HEALTH AS
SELECT
    W.PIPELINE_ID, C.PIPELINE_NAME, C.DOMAIN,
    W.WATERMARK_COL, W.LAST_VALUE_TS, W.LAST_VALUE_NUM,
    DATEDIFF('hour', W.LAST_VALUE_TS, CURRENT_TIMESTAMP()) AS HOURS_BEHIND,
    W.LAST_RUN_AT, W.LAST_RUN_STATUS, W.ROWS_LOADED
FROM ETL_AGENT_DB.ETL_META.ETL_WATERMARKS W
JOIN ETL_AGENT_DB.ETL_META.ETL_CONFIGS C ON C.PIPELINE_ID = W.PIPELINE_ID
ORDER BY HOURS_BEHIND DESC NULLS LAST;

-- View grants
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_PENDING_PIPELINES      TO ROLE ETL_AGENT_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_DEPLOYED_PIPELINES     TO ROLE ETL_AGENT_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_PIPELINE_SUMMARY       TO ROLE ETL_AGENT_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_LAST_RUN_PER_PIPELINE  TO ROLE ETL_AGENT_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_AUDIT_TRAIL            TO ROLE ETL_AGENT_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_WATERMARK_HEALTH       TO ROLE ETL_AGENT_ROLE;

GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_PENDING_PIPELINES      TO ROLE ETL_UI_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_PIPELINE_SUMMARY       TO ROLE ETL_UI_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_AUDIT_TRAIL            TO ROLE ETL_UI_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_LAST_RUN_PER_PIPELINE  TO ROLE ETL_UI_ROLE;
GRANT SELECT ON VIEW ETL_AGENT_DB.ETL_META.V_WATERMARK_HEALTH       TO ROLE ETL_UI_ROLE;


-- =============================================================================
-- SECTION 10: STORED PROCEDURES
-- =============================================================================

-- 10a. Approve
CREATE OR REPLACE PROCEDURE ETL_AGENT_DB.ETL_META.SP_APPROVE_PIPELINE(
    P_PIPELINE_ID    VARCHAR,
    P_GENERATED_SQL  TEXT,
    P_TASK_NAME      VARCHAR,
    P_APPROVED_BY    VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
DECLARE
    v_rows_updated NUMBER;
BEGIN
    UPDATE ETL_AGENT_DB.ETL_META.ETL_CONFIGS
    SET
        STATUS        = 'APPROVED',
        GENERATED_SQL = P_GENERATED_SQL,
        TASK_NAME     = P_TASK_NAME,
        APPROVED_BY   = P_APPROVED_BY,
        APPROVED_AT   = CURRENT_TIMESTAMP(),
        UPDATED_AT    = CURRENT_TIMESTAMP()
    WHERE PIPELINE_ID = P_PIPELINE_ID
      AND STATUS      = 'PENDING';
 
    v_rows_updated := SQLROWCOUNT;
 
    IF (v_rows_updated = 0) THEN
        RETURN 'ERROR: ' || P_PIPELINE_ID || ' not found or not in PENDING status.';
    END IF;
 
    RETURN 'OK: ' || P_PIPELINE_ID || ' approved by ' || P_APPROVED_BY;
END;
$$;

-- 10b. Deploy
CREATE OR REPLACE PROCEDURE ETL_AGENT_DB.ETL_META.SP_DEPLOY_PIPELINE(
    P_PIPELINE_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
DECLARE
    v_rows_updated NUMBER;
BEGIN
    UPDATE ETL_AGENT_DB.ETL_META.ETL_CONFIGS
    SET
        STATUS      = 'DEPLOYED',
        DEPLOYED_AT = CURRENT_TIMESTAMP(),
        UPDATED_AT  = CURRENT_TIMESTAMP()
    WHERE PIPELINE_ID = P_PIPELINE_ID
      AND STATUS      = 'APPROVED';
 
    v_rows_updated := SQLROWCOUNT;
 
    IF (v_rows_updated = 0) THEN
        RETURN 'ERROR: ' || P_PIPELINE_ID || ' not found or not in APPROVED status.';
    END IF;
 
    RETURN 'OK: ' || P_PIPELINE_ID || ' deployed.';
END;
$$;

-- 10c. Fail
CREATE OR REPLACE PROCEDURE ETL_AGENT_DB.ETL_META.SP_FAIL_PIPELINE(
    P_PIPELINE_ID VARCHAR, 
    P_ERROR_MESSAGE TEXT
)
RETURNS VARCHAR 
LANGUAGE SQL 
AS $$
BEGIN
    UPDATE ETL_AGENT_DB.ETL_META.ETL_CONFIGS
    SET STATUS = 'FAILED', 
        ERROR_MESSAGE = P_ERROR_MESSAGE, 
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE PIPELINE_ID = P_PIPELINE_ID;
    
    RETURN 'OK: ' || P_PIPELINE_ID || ' marked FAILED.';
END;
$$;

-- 10d. Reject
CREATE OR REPLACE PROCEDURE ETL_AGENT_DB.ETL_META.SP_REJECT_PIPELINE(
    P_PIPELINE_ID VARCHAR, 
    P_REJECTED_BY VARCHAR, 
    P_REASON TEXT
)
RETURNS VARCHAR 
LANGUAGE SQL 
AS $$
BEGIN
    UPDATE ETL_AGENT_DB.ETL_META.ETL_CONFIGS
    SET STATUS = 'REJECTED',
        ERROR_MESSAGE = 'Rejected by ' || P_REJECTED_BY || ': ' || P_REASON,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE PIPELINE_ID = P_PIPELINE_ID AND STATUS = 'PENDING';
    
    -- Check if the update actually found a 'PENDING' record
    IF (SQLROWCOUNT = 0) THEN 
        RETURN 'ERROR: ' || P_PIPELINE_ID || ' not found or not PENDING.'; 
    END IF;

    RETURN 'OK: ' || P_PIPELINE_ID || ' rejected by ' || P_REJECTED_BY;
END;
$$;

-- 10e. Reset (move FAILED/REJECTED back to PENDING for retry)
CREATE OR REPLACE PROCEDURE ETL_AGENT_DB.ETL_META.SP_RESET_PIPELINE(P_PIPELINE_ID VARCHAR)
RETURNS VARCHAR 
LANGUAGE SQL 
AS $$
BEGIN
    UPDATE ETL_AGENT_DB.ETL_META.ETL_CONFIGS
    SET STATUS = 'PENDING', 
        ERROR_MESSAGE = NULL, 
        GENERATED_SQL = NULL, 
        TASK_NAME = NULL,
        APPROVED_BY = NULL, 
        APPROVED_AT = NULL, 
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE PIPELINE_ID = P_PIPELINE_ID AND STATUS IN ('FAILED', 'REJECTED');
    
    IF (SQLROWCOUNT = 0) THEN 
        RETURN 'ERROR: ' || P_PIPELINE_ID || ' not in FAILED/REJECTED.'; 
    END IF;
    
    RETURN 'OK: ' || P_PIPELINE_ID || ' reset to PENDING.';
END;
$$;

GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_APPROVE_PIPELINE(VARCHAR,TEXT,VARCHAR,VARCHAR) TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_DEPLOY_PIPELINE(VARCHAR)                      TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_FAIL_PIPELINE(VARCHAR,TEXT)                   TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_REJECT_PIPELINE(VARCHAR,VARCHAR,TEXT)         TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_RESET_PIPELINE(VARCHAR)                       TO ROLE ETL_AGENT_ROLE;
GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_RESET_PIPELINE(VARCHAR)                       TO ROLE ETL_UI_ROLE;


-- =============================================================================
-- SECTION 11: GET_DDL ACCESS
-- =============================================================================

GRANT USAGE      ON DATABASE FINANCE_DB                         TO ROLE ETL_AGENT_ROLE;
GRANT USAGE      ON ALL SCHEMAS IN DATABASE FINANCE_DB          TO ROLE ETL_AGENT_ROLE;
GRANT REFERENCES ON ALL TABLES IN SCHEMA FINANCE_DB.RAW         TO ROLE ETL_AGENT_ROLE;
GRANT REFERENCES ON ALL TABLES IN SCHEMA FINANCE_DB.CLEAN       TO ROLE ETL_AGENT_ROLE;

-- Template for additional domains:
-- GRANT USAGE      ON DATABASE <NEW_DB>                        TO ROLE ETL_AGENT_ROLE;
-- GRANT USAGE      ON ALL SCHEMAS IN DATABASE <NEW_DB>         TO ROLE ETL_AGENT_ROLE;
-- GRANT REFERENCES ON ALL TABLES IN SCHEMA <NEW_DB>.<SCHEMA>  TO ROLE ETL_AGENT_ROLE;


-- =============================================================================
-- SECTION 12: VERIFICATION + SMOKE TEST
-- =============================================================================

SHOW TABLES     IN SCHEMA ETL_AGENT_DB.ETL_META;
SHOW VIEWS      IN SCHEMA ETL_AGENT_DB.ETL_META;
SHOW PROCEDURES IN SCHEMA ETL_AGENT_DB.ETL_META;

-- Smoke test: insert a PENDING pipeline and verify JSON parsing
INSERT INTO ETL_AGENT_DB.ETL_META.ETL_CONFIGS (
    PIPELINE_ID, PIPELINE_NAME, DOMAIN,
    SOURCE_TABLE, TARGET_TABLE, LOAD_STRATEGY,
    PIPELINE_PARAMS, STATUS, CREATED_BY
) 
SELECT 
    'ETL_SMOKE_TEST_001',
    'smoke_test_pipeline',
    'finance',
    'FINANCE_DB.RAW.TRANSACTIONS_RAW',
    'FINANCE_DB.CLEAN.TRANSACTIONS_CLEAN',
    'INCREMENTAL',
    PARSE_JSON('{
        "identity":        {"pipeline_name":"smoke_test_pipeline","domain":"finance","owner_email":"test@test.com"},
        "source":          {"table":"FINANCE_DB.RAW.TRANSACTIONS_RAW","filter":null,"deduplicate":false,"dedup_key":null,"dedup_order_by":null},
        "target":          {"table":"FINANCE_DB.CLEAN.TRANSACTIONS_CLEAN","write_mode":"append"},
        "strategy":        {"type":"INCREMENTAL","watermark_col":"UPDATED_AT","watermark_initial":"2020-01-01","merge_keys":[],"delete_flag_col":null},
        "transformations": {"nl_rules":"Smoke test.","column_mappings":[]},
        "execution":       {"warehouse":"ETL_AGENT_WH","schedule":"1 HOUR","timeout_minutes":30,"max_retries":2},
        "quality":         {"error_tolerance_pct":1.0,"null_check_cols":["TRANSACTION_ID"],"min_rows_expected":0,"notification_email":null},
        "hooks":           {"pre_load_sql":null,"post_load_sql":null}
    }'),
    'PENDING',
    CURRENT_USER();

-- Verify view + JSON parsing
SELECT
    PIPELINE_ID,
    PIPELINE_PARAMS:identity:domain::VARCHAR           AS domain,
    PIPELINE_PARAMS:source:table::VARCHAR              AS source_table,
    PIPELINE_PARAMS:strategy:type::VARCHAR             AS strategy,
    PIPELINE_PARAMS:strategy:watermark_col::VARCHAR    AS watermark_col,
    PIPELINE_PARAMS:execution:schedule::VARCHAR        AS schedule,
    PIPELINE_PARAMS:quality:error_tolerance_pct::FLOAT AS error_tolerance
FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS
WHERE PIPELINE_ID = 'ETL_SMOKE_TEST_001';

-- Clean up
DELETE FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS WHERE PIPELINE_ID = 'ETL_SMOKE_TEST_001';

SELECT 'setup.sql complete — infrastructure ready.' AS STATUS;