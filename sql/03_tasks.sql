-- =============================================================================
-- ETL AGENT — step4_tasks.sql
-- Snowflake Task templates for all 3 strategies.
-- All configuration is read from PIPELINE_PARAMS VARIANT at runtime.
-- Templates use {{PLACEHOLDERS}} replaced by the agent before execution.
-- Commented blocks = reference patterns. Executable blocks = run directly.
-- =============================================================================

USE DATABASE ETL_AGENT_DB;
USE SCHEMA   ETL_META;
USE WAREHOUSE ETL_AGENT_WH;


-- =============================================================================
-- SECTION 1: HELPER PROCEDURE — QUALITY CHECK
-- Called inside every task after the main load.
-- Reads quality thresholds from PIPELINE_PARAMS and fails the task if
-- error tolerance, null checks, or row count checks are violated.
-- =============================================================================

CREATE OR REPLACE PROCEDURE ETL_AGENT_DB.ETL_META.SP_RUN_QUALITY_CHECK(
    P_PIPELINE_ID    VARCHAR,
    P_TARGET_TABLE   VARCHAR,
    P_ROWS_LOADED    NUMBER,
    P_RUN_ID         VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
DECLARE
    v_params             VARIANT;
    v_error_tol          FLOAT;
    v_min_rows           NUMBER;
    v_null_cols          VARIANT;
    v_null_cols_size     NUMBER DEFAULT 0;
    v_null_col           VARCHAR;
    v_null_count         NUMBER;
    v_total_rows         NUMBER;
    v_null_pct           FLOAT;
    v_issues             VARCHAR DEFAULT '';
    v_status             VARCHAR DEFAULT 'PASS';
    v_check_sql          VARCHAR;
    i                    NUMBER DEFAULT 0;
    
    -- Required for executing dynamic SQL and returning a value
    rs                   RESULTSET;
    c1                   CURSOR FOR rs;
BEGIN
    -- Read quality thresholds from PIPELINE_PARAMS using bind variables (:)
    SELECT PIPELINE_PARAMS:quality
    INTO   :v_params
    FROM   ETL_AGENT_DB.ETL_META.ETL_CONFIGS
    WHERE  PIPELINE_ID = :P_PIPELINE_ID;

    v_error_tol := v_params:error_tolerance_pct::FLOAT;
    v_min_rows  := v_params:min_rows_expected::NUMBER;
    v_null_cols := v_params:null_check_cols;

    -- Check 1: Minimum row count
    IF (v_min_rows > 0 AND P_ROWS_LOADED < v_min_rows) THEN
        v_status := 'FAIL';
        v_issues := v_issues || 'MIN_ROWS_FAIL: loaded=' || P_ROWS_LOADED::VARCHAR
                    || ' expected>=' || v_min_rows::VARCHAR || '; ';
    END IF;

    -- Check 2: Null check on critical columns
    IF (v_null_cols IS NOT NULL) THEN
        -- Safely evaluate array size via SQL
        SELECT ARRAY_SIZE(:v_null_cols) INTO :v_null_cols_size;

        IF (v_null_cols_size > 0) THEN
            i := 0;
            WHILE (i < v_null_cols_size) DO
                -- Safely extract the column name from the variant array
                SELECT GET(:v_null_cols, :i)::VARCHAR INTO :v_null_col;
                
                -- Construct dynamic SQL
                v_check_sql := 'SELECT COUNT(*) FROM ' || P_TARGET_TABLE || ' WHERE ' || v_null_col || ' IS NULL';
                
                -- Execute dynamic SQL via RESULTSET and CURSOR
                rs := (EXECUTE IMMEDIATE :v_check_sql);
                OPEN c1;
                FETCH c1 INTO v_null_count;
                CLOSE c1;

                IF (P_ROWS_LOADED > 0) THEN
                    v_null_pct := (v_null_count / P_ROWS_LOADED) * 100;
                    IF (v_null_pct > v_error_tol) THEN
                        v_status := 'FAIL';
                        v_issues := v_issues || 'NULL_FAIL: col=' || v_null_col
                                    || ' null_pct=' || v_null_pct::VARCHAR
                                    || '% (tolerance=' || v_error_tol::VARCHAR || '%); ';
                    END IF;
                END IF;
                
                i := i + 1;
            END WHILE;
        END IF;
    END IF;

    -- Update run log with quality result
    UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
    SET
        QUALITY_CHECK_STATUS = :v_status,
        QUALITY_CHECK_DETAIL = CASE WHEN :v_issues = '' THEN 'All checks passed'
                                    ELSE :v_issues END
    WHERE RUN_ID = :P_RUN_ID;

    -- Return status — task uses this to decide whether to raise an error
    IF (v_status = 'FAIL') THEN
        RETURN 'FAIL: ' || v_issues;
    END IF;
    
    RETURN 'PASS';
END;
$$;

GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_RUN_QUALITY_CHECK(
    VARCHAR, VARCHAR, NUMBER, VARCHAR
) TO ROLE ETL_AGENT_ROLE;


-- =============================================================================
-- SECTION 2: HELPER PROCEDURE — EXECUTE HOOK
-- Runs a pre_load_sql or post_load_sql string from PIPELINE_PARAMS.
-- Called by the task before and after the main load block.
-- =============================================================================

CREATE OR REPLACE PROCEDURE ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK(
    P_PIPELINE_ID  VARCHAR,
    P_HOOK_TYPE    VARCHAR,   -- 'pre' or 'post'
    P_RUN_ID       VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
DECLARE
    v_sql    VARCHAR;
    v_col    VARCHAR DEFAULT 'PRE_HOOK_STATUS';
BEGIN
    -- Read the hook SQL from PIPELINE_PARAMS
    IF (P_HOOK_TYPE = 'pre') THEN
        SELECT PIPELINE_PARAMS:hooks:pre_load_sql::VARCHAR
        INTO   :v_sql
        FROM   ETL_AGENT_DB.ETL_META.ETL_CONFIGS
        WHERE  PIPELINE_ID = P_PIPELINE_ID;
        v_col := 'PRE_HOOK_STATUS';
    ELSE
        SELECT PIPELINE_PARAMS:hooks:post_load_sql::VARCHAR
        INTO   :v_sql
        FROM   ETL_AGENT_DB.ETL_META.ETL_CONFIGS
        WHERE  PIPELINE_ID = P_PIPELINE_ID;
        v_col := 'POST_HOOK_STATUS';
    END IF;

    -- Skip if no hook defined
    IF (v_sql IS NULL OR v_sql = '') THEN
        UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        SET PRE_HOOK_STATUS  = CASE WHEN P_HOOK_TYPE = 'pre'  THEN 'SKIPPED' ELSE PRE_HOOK_STATUS  END,
            POST_HOOK_STATUS = CASE WHEN P_HOOK_TYPE = 'post' THEN 'SKIPPED' ELSE POST_HOOK_STATUS END
        WHERE RUN_ID = P_RUN_ID;
        RETURN 'SKIPPED';
    END IF;

    -- Execute hook SQL
    EXECUTE IMMEDIATE :v_sql;

    UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
    SET PRE_HOOK_STATUS  = CASE WHEN P_HOOK_TYPE = 'pre'  THEN 'SUCCESS' ELSE PRE_HOOK_STATUS  END,
        POST_HOOK_STATUS = CASE WHEN P_HOOK_TYPE = 'post' THEN 'SUCCESS' ELSE POST_HOOK_STATUS END
    WHERE RUN_ID = P_RUN_ID;

    RETURN 'SUCCESS';

EXCEPTION
    WHEN OTHER THEN
        UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        SET PRE_HOOK_STATUS  = CASE WHEN P_HOOK_TYPE = 'pre'  THEN 'FAILED: ' || SQLERRM ELSE PRE_HOOK_STATUS  END,
            POST_HOOK_STATUS = CASE WHEN P_HOOK_TYPE = 'post' THEN 'FAILED: ' || SQLERRM ELSE POST_HOOK_STATUS END
        WHERE RUN_ID = P_RUN_ID;
        RAISE;
END;
$$;

GRANT USAGE ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK(
    VARCHAR, VARCHAR, VARCHAR
) TO ROLE ETL_AGENT_ROLE;


-- =============================================================================
-- SECTION 3: TASK TEMPLATES
-- The agent reads these patterns from SKILL.md and generates real versions.
-- Every {{PLACEHOLDER}} is replaced with real values at deploy time.
-- Blocks wrapped in /* */ are reference patterns — not executed directly.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- TEMPLATE A: INCREMENTAL
-- Placeholders:
--   {{TASK_NAME}}          e.g. TASK_TRANSACTIONS_RAW_TO_CLEAN
--   {{TASK_SCHEDULE}}      e.g. '1 HOUR'
--   {{WAREHOUSE}}          e.g. ETL_AGENT_WH
--   {{PIPELINE_ID}}        e.g. ETL_TRANSACTIONS_20240101120000
--   {{SOURCE_TABLE}}       fully qualified
--   {{TARGET_TABLE}}       fully qualified
--   {{WATERMARK_COL}}      e.g. UPDATED_AT
--   {{TARGET_COL_LIST}}    comma-separated target columns in DDL order
--   {{SELECT_EXPRESSIONS}} agent-generated SELECT list with all NL transforms
--   {{SOURCE_FILTER}}      permanent WHERE clause (or '1=1' if none)
--   {{DEDUP_BLOCK}}        QUALIFY ROW_NUMBER()... block (or empty if no dedup)
-- -----------------------------------------------------------------------------
/*
CREATE OR REPLACE TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}}
    WAREHOUSE  = {{WAREHOUSE}}
    SCHEDULE   = '{{TASK_SCHEDULE}}'
    COMMENT    = 'ETL Agent | pipeline:{{PIPELINE_ID}} | strategy:INCREMENTAL'
AS
DECLARE
    v_watermark_from  TIMESTAMP_NTZ;
    v_watermark_to    TIMESTAMP_NTZ;
    v_rows_loaded     NUMBER;
    v_run_id          VARCHAR;
    v_query_id        VARCHAR;
    v_quality_result  VARCHAR;
    v_hook_result     VARCHAR;
BEGIN
    -- 1. Read current watermark (epoch on first run)
    SELECT COALESCE(LAST_VALUE_TS, '{{WATERMARK_INITIAL}}'::TIMESTAMP_NTZ)
    INTO   :v_watermark_from
    FROM   ETL_AGENT_DB.ETL_META.ETL_WATERMARKS
    WHERE  PIPELINE_ID = '{{PIPELINE_ID}}';

    -- 2. Open run log
    SET v_run_id := CONCAT('RUN_', TO_VARCHAR(CURRENT_TIMESTAMP(),'YYYYMMDD_HH24MISS_FF3'));
    INSERT INTO ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        (RUN_ID, PIPELINE_ID, TASK_NAME, LOAD_STRATEGY, RUN_START_AT, STATUS, WATERMARK_FROM)
    VALUES (:v_run_id,'{{PIPELINE_ID}}','{{TASK_NAME}}','INCREMENTAL',
            CURRENT_TIMESTAMP(),'RUNNING', TO_VARCHAR(:v_watermark_from));

    -- 3. Pre-load hook
    CALL ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK('{{PIPELINE_ID}}','pre',:v_run_id);

    -- 4. Main load — agent fills {{SELECT_EXPRESSIONS}} and {{TARGET_COL_LIST}}
    INSERT INTO {{TARGET_TABLE}} (
        {{TARGET_COL_LIST}}
    )
    SELECT
        {{SELECT_EXPRESSIONS}}
    FROM {{SOURCE_TABLE}} src
    WHERE src.{{WATERMARK_COL}} > :v_watermark_from
      AND src.{{WATERMARK_COL}} IS NOT NULL
      AND {{SOURCE_FILTER}}
    {{DEDUP_BLOCK}};

    -- 5. Capture stats
    SET v_query_id := LAST_QUERY_ID();
    SELECT COUNT(*) INTO :v_rows_loaded FROM TABLE(RESULT_SCAN(:v_query_id));

    -- 6. Find new high-water mark
    SELECT MAX(src.{{WATERMARK_COL}})
    INTO   :v_watermark_to
    FROM   {{SOURCE_TABLE}} src
    WHERE  src.{{WATERMARK_COL}} > :v_watermark_from;

    -- 7. Upsert watermark
    MERGE INTO ETL_AGENT_DB.ETL_META.ETL_WATERMARKS tgt
    USING (SELECT '{{PIPELINE_ID}}' AS PID, :v_watermark_to AS WM) src
    ON tgt.PIPELINE_ID = src.PID
    WHEN MATCHED     THEN UPDATE SET
        LAST_VALUE_TS   = src.WM,
        LAST_VALUE      = TO_VARCHAR(src.WM),
        LAST_RUN_AT     = CURRENT_TIMESTAMP(),
        LAST_RUN_STATUS = 'SUCCESS',
        ROWS_LOADED     = :v_rows_loaded,
        UPDATED_AT      = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (PIPELINE_ID, WATERMARK_COL, LAST_VALUE_TS, LAST_VALUE,
         LAST_RUN_AT, LAST_RUN_STATUS, ROWS_LOADED)
    VALUES (src.PID,'{{WATERMARK_COL}}',src.WM,TO_VARCHAR(src.WM),
            CURRENT_TIMESTAMP(),'SUCCESS',:v_rows_loaded);

    -- 8. Post-load hook
    CALL ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK('{{PIPELINE_ID}}','post',:v_run_id);

    -- 9. Quality check
    SET v_quality_result := (
        CALL ETL_AGENT_DB.ETL_META.SP_RUN_QUALITY_CHECK(
            '{{PIPELINE_ID}}','{{TARGET_TABLE}}',:v_rows_loaded,:v_run_id
        )
    );

    -- 10. Close run log
    UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
    SET STATUS         = CASE WHEN :v_quality_result LIKE 'FAIL%' THEN 'QUALITY_FAIL' ELSE 'SUCCESS' END,
        RUN_END_AT     = CURRENT_TIMESTAMP(),
        ROWS_PROCESSED = :v_rows_loaded,
        WATERMARK_TO   = TO_VARCHAR(:v_watermark_to),
        QUERY_ID       = :v_query_id
    WHERE RUN_ID = :v_run_id;

    -- 11. Raise if quality failed (makes Snowflake mark the task run as FAILED)
    IF (:v_quality_result LIKE 'FAIL%') THEN
        RAISE EXCEPTION 'Quality check failed: %', :v_quality_result;
    END IF;

EXCEPTION
    WHEN OTHER THEN
        UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        SET STATUS        = 'FAILED',
            RUN_END_AT    = CURRENT_TIMESTAMP(),
            ERROR_MESSAGE = SQLERRM
        WHERE RUN_ID = :v_run_id;

        -- Reset watermark to before this run on failure (safe re-run)
        UPDATE ETL_AGENT_DB.ETL_META.ETL_WATERMARKS
        SET LAST_RUN_STATUS = 'FAILED', UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE PIPELINE_ID = '{{PIPELINE_ID}}';
        RAISE;
END;
*/


-- -----------------------------------------------------------------------------
-- TEMPLATE B: FULL LOAD
-- Additional placeholder:
--   {{TARGET_COL_LIST}}     comma-separated target columns in DDL order
--   {{SELECT_EXPRESSIONS}}  agent-generated SELECT list with all NL transforms
--   {{SOURCE_FILTER}}       permanent WHERE clause (or '1=1' if none)
-- -----------------------------------------------------------------------------
/*
CREATE OR REPLACE TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}}
    WAREHOUSE  = {{WAREHOUSE}}
    SCHEDULE   = '{{TASK_SCHEDULE}}'
    COMMENT    = 'ETL Agent | pipeline:{{PIPELINE_ID}} | strategy:FULL_LOAD'
AS
DECLARE
    v_rows_loaded    NUMBER;
    v_run_id         VARCHAR;
    v_query_id       VARCHAR;
    v_quality_result VARCHAR;
BEGIN
    -- 1. Open run log
    SET v_run_id := CONCAT('RUN_', TO_VARCHAR(CURRENT_TIMESTAMP(),'YYYYMMDD_HH24MISS_FF3'));
    INSERT INTO ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        (RUN_ID, PIPELINE_ID, TASK_NAME, LOAD_STRATEGY, RUN_START_AT, STATUS)
    VALUES (:v_run_id,'{{PIPELINE_ID}}','{{TASK_NAME}}','FULL_LOAD',
            CURRENT_TIMESTAMP(),'RUNNING');

    -- 2. Pre-load hook (e.g. truncate staging)
    CALL ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK('{{PIPELINE_ID}}','pre',:v_run_id);

    -- 3. Truncate and reload
    INSERT OVERWRITE INTO {{TARGET_TABLE}} (
        {{TARGET_COL_LIST}}
    )
    SELECT
        {{SELECT_EXPRESSIONS}}
    FROM {{SOURCE_TABLE}} src
    WHERE {{SOURCE_FILTER}}
    {{DEDUP_BLOCK}};

    -- 4. Capture stats
    SET v_query_id := LAST_QUERY_ID();
    SELECT COUNT(*) INTO :v_rows_loaded FROM TABLE(RESULT_SCAN(:v_query_id));

    -- 5. Post-load hook
    CALL ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK('{{PIPELINE_ID}}','post',:v_run_id);

    -- 6. Quality check
    SET v_quality_result := (
        CALL ETL_AGENT_DB.ETL_META.SP_RUN_QUALITY_CHECK(
            '{{PIPELINE_ID}}','{{TARGET_TABLE}}',:v_rows_loaded,:v_run_id
        )
    );

    -- 7. Close run log
    UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
    SET STATUS         = CASE WHEN :v_quality_result LIKE 'FAIL%' THEN 'QUALITY_FAIL' ELSE 'SUCCESS' END,
        RUN_END_AT     = CURRENT_TIMESTAMP(),
        ROWS_PROCESSED = :v_rows_loaded,
        QUERY_ID       = :v_query_id
    WHERE RUN_ID = :v_run_id;

    IF (:v_quality_result LIKE 'FAIL%') THEN
        RAISE EXCEPTION 'Quality check failed: %', :v_quality_result;
    END IF;

EXCEPTION
    WHEN OTHER THEN
        UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        SET STATUS = 'FAILED', RUN_END_AT = CURRENT_TIMESTAMP(), ERROR_MESSAGE = SQLERRM
        WHERE RUN_ID = :v_run_id;
        RAISE;
END;
*/


-- -----------------------------------------------------------------------------
-- TEMPLATE C: CDC / MERGE
-- Additional placeholders:
--   {{MERGE_JOIN_CONDITION}}  e.g. tgt.TRANSACTION_ID = src.TRANSACTION_ID
--   {{MERGE_UPDATE_SET}}      all non-key target columns = src.col, ...
--   {{MERGE_INSERT_COLS}}     all target columns
--   {{MERGE_INSERT_VALUES}}   corresponding src expressions
--   {{DELETE_BRANCH}}         WHEN MATCHED AND src.IS_DELETED = TRUE THEN DELETE
--                             (omitted entirely if no delete_flag_col)
-- -----------------------------------------------------------------------------
/*
CREATE OR REPLACE TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}}
    WAREHOUSE  = {{WAREHOUSE}}
    SCHEDULE   = '{{TASK_SCHEDULE}}'
    COMMENT    = 'ETL Agent | pipeline:{{PIPELINE_ID}} | strategy:CDC'
AS
DECLARE
    v_rows_loaded    NUMBER;
    v_run_id         VARCHAR;
    v_query_id       VARCHAR;
    v_quality_result VARCHAR;
BEGIN
    -- 1. Open run log
    SET v_run_id := CONCAT('RUN_', TO_VARCHAR(CURRENT_TIMESTAMP(),'YYYYMMDD_HH24MISS_FF3'));
    INSERT INTO ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        (RUN_ID, PIPELINE_ID, TASK_NAME, LOAD_STRATEGY, RUN_START_AT, STATUS)
    VALUES (:v_run_id,'{{PIPELINE_ID}}','{{TASK_NAME}}','CDC',
            CURRENT_TIMESTAMP(),'RUNNING');

    -- 2. Pre-load hook
    CALL ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK('{{PIPELINE_ID}}','pre',:v_run_id);

    -- 3. MERGE — agent fills all {{PLACEHOLDERS}} below
    MERGE INTO {{TARGET_TABLE}} AS tgt
    USING (
        SELECT
            {{SELECT_EXPRESSIONS}}
        FROM {{SOURCE_TABLE}} src
        WHERE {{SOURCE_FILTER}}
        {{DEDUP_BLOCK}}
    ) AS src
    ON {{MERGE_JOIN_CONDITION}}

    {{DELETE_BRANCH}}

    WHEN MATCHED THEN UPDATE SET
        {{MERGE_UPDATE_SET}},
        LOADED_AT = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        {{MERGE_INSERT_COLS}}
    ) VALUES (
        {{MERGE_INSERT_VALUES}}
    );

    -- 4. Capture stats
    SET v_query_id := LAST_QUERY_ID();

    -- 5. Row count from target after merge
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM {{TARGET_TABLE}}'
    INTO v_rows_loaded;

    -- 6. Post-load hook
    CALL ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK('{{PIPELINE_ID}}','post',:v_run_id);

    -- 7. Quality check
    SET v_quality_result := (
        CALL ETL_AGENT_DB.ETL_META.SP_RUN_QUALITY_CHECK(
            '{{PIPELINE_ID}}','{{TARGET_TABLE}}',:v_rows_loaded,:v_run_id
        )
    );

    -- 8. Close run log
    UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
    SET STATUS         = CASE WHEN :v_quality_result LIKE 'FAIL%' THEN 'QUALITY_FAIL' ELSE 'SUCCESS' END,
        RUN_END_AT     = CURRENT_TIMESTAMP(),
        ROWS_PROCESSED = :v_rows_loaded,
        QUERY_ID       = :v_query_id
    WHERE RUN_ID = :v_run_id;

    IF (:v_quality_result LIKE 'FAIL%') THEN
        RAISE EXCEPTION 'Quality check failed: %', :v_quality_result;
    END IF;

EXCEPTION
    WHEN OTHER THEN
        UPDATE ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG
        SET STATUS = 'FAILED', RUN_END_AT = CURRENT_TIMESTAMP(), ERROR_MESSAGE = SQLERRM
        WHERE RUN_ID = :v_run_id;
        RAISE;
END;
*/


-- =============================================================================
-- SECTION 4: DEDUP BLOCK REFERENCE
-- The agent inserts one of these snippets as {{DEDUP_BLOCK}}
-- when source.deduplicate = true in PIPELINE_PARAMS.
-- If deduplicate = false, {{DEDUP_BLOCK}} is replaced with empty string.
-- =============================================================================

-- Pattern for INCREMENTAL and FULL_LOAD (appended after WHERE):
/*
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.{{DEDUP_KEY}}
    ORDER BY {{DEDUP_ORDER_BY}}
) = 1
*/

-- Pattern for CDC (inside the USING subquery, before closing paren):
/*
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.{{DEDUP_KEY}}
    ORDER BY {{DEDUP_ORDER_BY}}
) = 1
*/


-- =============================================================================
-- SECTION 5: AGENT DEPLOYMENT EXECUTION SEQUENCE
-- Exact steps the agent runs after human types Y.
-- Agent substitutes real values for every {{PLACEHOLDER}}.
-- Run one statement at a time — stop on any error.
-- =============================================================================

-- Step 5.1 — Mark APPROVED, store generated SQL + task name
/*
CALL ETL_AGENT_DB.ETL_META.SP_APPROVE_PIPELINE(
    '{{PIPELINE_ID}}',
    '{{GENERATED_SQL_ESCAPED}}',
    '{{TASK_NAME}}',
    CURRENT_USER()
);
-- Verify return value starts with 'OK:' before proceeding
*/

-- Step 5.2 — Execute the fully rendered CREATE OR REPLACE TASK
/*
<<FULLY_RENDERED_CREATE_TASK_SQL>>
-- No {{PLACEHOLDERS}} should remain at this point
*/

-- Step 5.3 — Seed watermark (INCREMENTAL only — skip for FULL_LOAD and CDC)
/*
MERGE INTO ETL_AGENT_DB.ETL_META.ETL_WATERMARKS tgt
USING (SELECT
           '{{PIPELINE_ID}}'                              AS PID,
           '{{WATERMARK_COL}}'                            AS WCOL,
           '{{WATERMARK_INITIAL}}'::TIMESTAMP_NTZ         AS WTS
      ) src
ON tgt.PIPELINE_ID = src.PID
WHEN NOT MATCHED THEN INSERT
    (PIPELINE_ID, WATERMARK_COL, LAST_VALUE_TS, LAST_VALUE,
     LAST_RUN_AT, LAST_RUN_STATUS, ROWS_LOADED)
VALUES
    (src.PID, src.WCOL, src.WTS, '{{WATERMARK_INITIAL}}',
     CURRENT_TIMESTAMP(), 'INIT', 0);
*/

-- Step 5.4 — Resume the task
/*
ALTER TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}} RESUME;
*/

-- Step 5.5 — Verify task is active (STATE should be 'started')
/*
SHOW TASKS LIKE '{{TASK_NAME}}' IN SCHEMA ETL_AGENT_DB.ETL_META;
*/

-- Step 5.6 — Mark DEPLOYED
/*
CALL ETL_AGENT_DB.ETL_META.SP_DEPLOY_PIPELINE('{{PIPELINE_ID}}');
*/

-- Step 5.7 — On any failure in steps 5.2–5.6, agent calls this instead:
/*
CALL ETL_AGENT_DB.ETL_META.SP_FAIL_PIPELINE(
    '{{PIPELINE_ID}}',
    '{{ERROR_FROM_FAILED_STEP}}'
);
*/


-- =============================================================================
-- SECTION 6: TASK MANAGEMENT UTILITIES
-- Used by agent for maintenance + engineers for manual operations.
-- =============================================================================

-- Suspend a task (e.g. during incident)
/*
ALTER TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}} SUSPEND;
*/

-- Resume a suspended task
/*
ALTER TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}} RESUME;
*/

-- Force an immediate manual run outside the schedule
/*
EXECUTE TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}};
*/

-- Decommission a pipeline (suspend task + update status)
/*
ALTER TASK ETL_AGENT_DB.ETL_META.{{TASK_NAME}} SUSPEND;
DROP TASK  ETL_AGENT_DB.ETL_META.{{TASK_NAME}};

UPDATE ETL_AGENT_DB.ETL_META.ETL_CONFIGS
SET STATUS = 'DECOMMISSIONED', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PIPELINE_ID = '{{PIPELINE_ID}}';

DELETE FROM ETL_AGENT_DB.ETL_META.ETL_WATERMARKS
WHERE PIPELINE_ID = '{{PIPELINE_ID}}';
*/

-- Reset watermark to backfill from a specific date
/*
UPDATE ETL_AGENT_DB.ETL_META.ETL_WATERMARKS
SET LAST_VALUE_TS   = '{{BACKFILL_FROM_DATE}}'::TIMESTAMP_NTZ,
    LAST_VALUE      = '{{BACKFILL_FROM_DATE}}',
    LAST_RUN_STATUS = 'RESET',
    UPDATED_AT      = CURRENT_TIMESTAMP()
WHERE PIPELINE_ID = '{{PIPELINE_ID}}';
*/

-- Check Snowflake native task run history
/*
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE,
    DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SECS
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
        TASK_NAME => '{{TASK_NAME}}'
    )
)
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;
*/


-- =============================================================================
-- SECTION 7: MONITORING QUERIES
-- Run these to inspect pipeline health. Bookmark these in Snowsight.
-- =============================================================================

-- All deployed pipelines with last run status
/*
SELECT
    C.PIPELINE_NAME,
    C.DOMAIN,
    C.LOAD_STRATEGY,
    C.TASK_NAME,
    C.PIPELINE_PARAMS:execution:schedule::VARCHAR        AS SCHEDULE,
    L.LAST_RUN_STATUS,
    L.RUN_START_AT                                       AS LAST_RUN_AT,
    L.ROWS_PROCESSED                                     AS LAST_RUN_ROWS,
    L.DURATION_SECONDS,
    L.WATERMARK_TO                                       AS CURRENT_WATERMARK,
    L.QUALITY_CHECK_STATUS,
    L.ERROR_MESSAGE
FROM ETL_AGENT_DB.ETL_META.ETL_CONFIGS C
LEFT JOIN ETL_AGENT_DB.ETL_META.V_LAST_RUN_PER_PIPELINE L
    ON L.PIPELINE_ID = C.PIPELINE_ID
WHERE C.STATUS = 'DEPLOYED'
ORDER BY C.DEPLOYED_AT DESC;
*/

-- Pipelines that failed or had quality issues in last 24 hours
/*
SELECT
    C.PIPELINE_NAME,
    C.DOMAIN,
    L.TASK_NAME,
    L.STATUS,
    L.RUN_START_AT,
    L.QUALITY_CHECK_STATUS,
    L.QUALITY_CHECK_DETAIL,
    L.ERROR_MESSAGE
FROM ETL_AGENT_DB.ETL_META.ETL_TASK_RUN_LOG L
JOIN ETL_AGENT_DB.ETL_META.ETL_CONFIGS C ON C.PIPELINE_ID = L.PIPELINE_ID
WHERE L.STATUS IN ('FAILED','QUALITY_FAIL')
  AND L.RUN_START_AT >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY L.RUN_START_AT DESC;
*/

-- Watermark lag — how far behind each incremental pipeline is
/*
SELECT * FROM ETL_AGENT_DB.ETL_META.V_WATERMARK_HEALTH
ORDER BY HOURS_BEHIND DESC;
*/

-- Full run history for a specific pipeline
/*
SELECT
    RUN_ID, RUN_START_AT, RUN_END_AT, STATUS,
    ROWS_PROCESSED, WATERMARK_FROM, WATERMARK_TO,
    PRE_HOOK_STATUS, POST_HOOK_STATUS,
    QUALITY_CHECK_STATUS, QUALITY_CHECK_DETAIL,
    DURATION_SECONDS, ERROR_MESSAGE
FROM ETL_AGENT_DB.ETL_META.V_LAST_RUN_PER_PIPELINE
WHERE PIPELINE_ID = '{{PIPELINE_ID}}';
*/


-- =============================================================================
-- SECTION 8: VERIFICATION — run after setup.sql
-- =============================================================================

-- Confirm helper procedures were created
SHOW PROCEDURES LIKE 'SP_%' IN SCHEMA ETL_AGENT_DB.ETL_META;

-- Confirm procedures are callable by agent role
SHOW GRANTS ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_RUN_QUALITY_CHECK(VARCHAR,VARCHAR,NUMBER,VARCHAR);
SHOW GRANTS ON PROCEDURE ETL_AGENT_DB.ETL_META.SP_EXECUTE_HOOK(VARCHAR,VARCHAR,VARCHAR);

SELECT 'step4_tasks.sql complete — task templates and helper procedures ready.' AS STATUS;