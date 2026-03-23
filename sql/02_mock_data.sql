-- =============================================================================
-- ETL AGENT — mock_data.sql
-- Finance domain mock data: 3 RAW source tables + 3 CLEAN target tables
-- Run this ONCE to set up test data before running the ETL agent.
-- =============================================================================

-- =============================================================================
-- SECTION 1: DATABASE + SCHEMA SETUP
-- =============================================================================

CREATE DATABASE IF NOT EXISTS FINANCE_DB;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.RAW
    COMMENT = 'Raw source tables — unprocessed, as-landed from source systems';

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.CLEAN
    COMMENT = 'Clean target tables — transformed, validated, analytics-ready';

USE DATABASE FINANCE_DB;


-- =============================================================================
-- SECTION 2: RAW SOURCE TABLES
-- These simulate exactly what arrives from upstream source systems —
-- messy data types, JSON blobs, inconsistent formats, PII, nulls.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2A. TRANSACTIONS_RAW
-- Simulates a payment processing system feed.
-- Problems baked in:
--   - tx_payload is a JSON blob (needs parsing)
--   - tx_date is VARCHAR in mixed formats (needs casting)
--   - account_number is plain PII (needs masking)
--   - status has TEST/CANCELLED rows (needs filtering)
--   - tx_amount has nulls (needs coalescing)
--   - UPDATED_AT is the watermark column
-- Strategy: INCREMENTAL on UPDATED_AT
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE FINANCE_DB.RAW.TRANSACTIONS_RAW (
    TRANSACTION_ID      VARCHAR(64)     NOT NULL,
    ACCOUNT_NUMBER      VARCHAR(32),                -- PII — needs masking
    TX_DATE             VARCHAR(32),                -- Mixed formats: 'YYYY-MM-DD', 'MM/DD/YYYY'
    TX_AMOUNT           FLOAT,                      -- Nullable — needs COALESCE
    CURRENCY_CODE       VARCHAR(8),
    TX_PAYLOAD          VARIANT,                    -- JSON blob — needs parsing
    STATUS              VARCHAR(32),                -- 'COMPLETED','PENDING','CANCELLED','TEST'
    SOURCE_SYSTEM       VARCHAR(64),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
INSERT INTO FINANCE_DB.RAW.TRANSACTIONS_RAW
    (TRANSACTION_ID, ACCOUNT_NUMBER, TX_DATE, TX_AMOUNT, CURRENCY_CODE,
     TX_PAYLOAD, STATUS, SOURCE_SYSTEM, CREATED_AT, UPDATED_AT)
SELECT 
    TRANSACTION_ID, 
    ACCOUNT_NUMBER, 
    TX_DATE, 
    TX_AMOUNT, 
    CURRENCY_CODE,
    PARSE_JSON(TX_PAYLOAD_STR), -- Apply PARSE_JSON here in the SELECT list
    STATUS, 
    SOURCE_SYSTEM, 
    CREATED_AT, 
    UPDATED_AT
FROM VALUES
    ('TXN-001', 'ACC-4532-7891-0023', '2024-01-15', 1250.75, 'USD',
     '{"merchant_name":"Amazon","merchant_category":"RETAIL","merchant_city":"Seattle","merchant_country":"US","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-15 09:23:11'::TIMESTAMP_NTZ, '2024-01-15 09:23:11'::TIMESTAMP_NTZ),

    ('TXN-002', 'ACC-9921-3344-5567', '2024-01-16', 89.99, 'USD',
     '{"merchant_name":"Spotify","merchant_category":"SUBSCRIPTION","merchant_city":"Stockholm","merchant_country":"SE","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-16 14:05:32'::TIMESTAMP_NTZ, '2024-01-16 14:05:32'::TIMESTAMP_NTZ),

    ('TXN-003', 'ACC-1122-8876-9900', '2024-01-17', NULL, 'EUR',
     '{"merchant_name":"Local Cafe","merchant_category":"FOOD","merchant_city":"Berlin","merchant_country":"DE","payment_method":"CONTACTLESS"}',
     'PENDING', 'PAYMENT_GATEWAY_V1', '2024-01-17 08:11:00'::TIMESTAMP_NTZ, '2024-01-17 08:11:00'::TIMESTAMP_NTZ),

    ('TXN-004', 'ACC-4532-7891-0023', '2024-01-17', 4500.00, 'USD',
     '{"merchant_name":"Delta Airlines","merchant_category":"TRAVEL","merchant_city":"Atlanta","merchant_country":"US","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-17 16:45:22'::TIMESTAMP_NTZ, '2024-01-17 16:45:22'::TIMESTAMP_NTZ),

    ('TXN-005', 'ACC-7743-2211-8854', '2024-01-18', 22.50, 'GBP',
     '{"merchant_name":"Uber","merchant_category":"TRANSPORT","merchant_city":"London","merchant_country":"GB","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-18 11:30:00'::TIMESTAMP_NTZ, '2024-01-18 11:30:00'::TIMESTAMP_NTZ),

    ('TXN-006', 'ACC-TEST-0000-0001', '2024-01-18', 999.00, 'USD',
     '{"merchant_name":"TEST_MERCHANT","merchant_category":"TEST","merchant_city":"NA","merchant_country":"NA","payment_method":"TEST"}',
     'TEST', 'PAYMENT_GATEWAY_V2', '2024-01-18 00:00:01'::TIMESTAMP_NTZ, '2024-01-18 00:00:01'::TIMESTAMP_NTZ),

    ('TXN-007', 'ACC-9921-3344-5567', '2024-01-19', 3200.00, 'USD',
     '{"merchant_name":"Marriott Hotels","merchant_category":"TRAVEL","merchant_city":"New York","merchant_country":"US","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-19 20:14:55'::TIMESTAMP_NTZ, '2024-01-19 20:14:55'::TIMESTAMP_NTZ),

    ('TXN-008', 'ACC-3388-1100-2244', '2024-01-19', 150.00, 'USD',
     '{"merchant_name":"Whole Foods","merchant_category":"GROCERY","merchant_city":"Austin","merchant_country":"US","payment_method":"CONTACTLESS"}',
     'CANCELLED', 'PAYMENT_GATEWAY_V1', '2024-01-19 13:22:10'::TIMESTAMP_NTZ, '2024-01-20 09:00:00'::TIMESTAMP_NTZ),

    ('TXN-009', 'ACC-1122-8876-9900', '2024-01-20', 78.40, 'EUR',
     '{"merchant_name":"MediaMarkt","merchant_category":"ELECTRONICS","merchant_city":"Munich","merchant_country":"DE","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-20 15:08:33'::TIMESTAMP_NTZ, '2024-01-20 15:08:33'::TIMESTAMP_NTZ),

    ('TXN-010', 'ACC-5566-9977-3312', '2024-01-21', 12500.00, 'USD',
     '{"merchant_name":"Charles Schwab","merchant_category":"INVESTMENT","merchant_city":"San Francisco","merchant_country":"US","payment_method":"WIRE"}',
     'COMPLETED', 'WIRE_TRANSFER_SYSTEM', '2024-01-21 10:00:00'::TIMESTAMP_NTZ, '2024-01-21 10:00:00'::TIMESTAMP_NTZ),

    ('TXN-011', 'ACC-4532-7891-0023', '2024-01-22', 45.00, 'USD',
     '{"merchant_name":"Netflix","merchant_category":"SUBSCRIPTION","merchant_city":"Los Gatos","merchant_country":"US","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-22 06:00:00'::TIMESTAMP_NTZ, '2024-01-22 06:00:00'::TIMESTAMP_NTZ),

    ('TXN-012', 'ACC-7743-2211-8854', '2024-01-22', 8900.50, 'GBP',
     '{"merchant_name":"Harrods","merchant_category":"LUXURY_RETAIL","merchant_city":"London","merchant_country":"GB","payment_method":"CARD"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V2', '2024-01-22 14:33:21'::TIMESTAMP_NTZ, '2024-01-22 14:33:21'::TIMESTAMP_NTZ),

    ('TXN-013', 'ACC-9921-3344-5567', '2024-01-23', 299.99, 'USD',
     '{"merchant_name":"Apple Store","merchant_category":"ELECTRONICS","merchant_city":"Cupertino","merchant_country":"US","payment_method":"CARD"}',
     'PENDING', 'PAYMENT_GATEWAY_V2', '2024-01-23 17:45:00'::TIMESTAMP_NTZ, '2024-01-23 17:45:00'::TIMESTAMP_NTZ),

    ('TXN-014', 'ACC-3388-1100-2244', '2024-01-24', 55.20, 'USD',
     '{"merchant_name":"Shell Gas","merchant_category":"FUEL","merchant_city":"Houston","merchant_country":"US","payment_method":"CONTACTLESS"}',
     'COMPLETED', 'PAYMENT_GATEWAY_V1', '2024-01-24 07:15:44'::TIMESTAMP_NTZ, '2024-01-24 07:15:44'::TIMESTAMP_NTZ),

    ('TXN-015', 'ACC-5566-9977-3312', '2024-01-25', 2100.00, 'USD',
     '{"merchant_name":"Fidelity Investments","merchant_category":"INVESTMENT","merchant_city":"Boston","merchant_country":"US","payment_method":"WIRE"}',
     'COMPLETED', 'WIRE_TRANSFER_SYSTEM', '2024-01-25 11:30:00'::TIMESTAMP_NTZ, '2024-01-25 11:30:00'::TIMESTAMP_NTZ)
     
AS t(TRANSACTION_ID, ACCOUNT_NUMBER, TX_DATE, TX_AMOUNT, CURRENCY_CODE,
     TX_PAYLOAD_STR, STATUS, SOURCE_SYSTEM, CREATED_AT, UPDATED_AT); -- Note the alias change here

-- -----------------------------------------------------------------------------
-- 2B. ACCOUNTS_RAW
-- Simulates a core banking system daily account snapshot.
-- Problems baked in:
--   - BALANCE is VARCHAR (source system exports as string with commas)
--   - RISK_SCORE is VARCHAR '1'-'10' (needs CAST to NUMBER)
--   - ACCOUNT_STATUS uses legacy codes: 'A','S','C','D' (needs decoding)
--   - INTEREST_RATE stored as string '3.50%' (needs stripping % and casting)
--   - OPENED_DATE in format 'DD-MON-YYYY' (needs casting)
--   - Full reload daily — no watermark needed
-- Strategy: FULL LOAD
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE FINANCE_DB.RAW.ACCOUNTS_RAW (
    ACCOUNT_ID          VARCHAR(32)     NOT NULL,
    CUSTOMER_ID         VARCHAR(64),
    ACCOUNT_TYPE        VARCHAR(32),                -- 'CHECKING','SAVINGS','INVESTMENT','CREDIT'
    ACCOUNT_STATUS      VARCHAR(4),                 -- Legacy codes: 'A'=Active,'S'=Suspended,'C'=Closed,'D'=Dormant
    BALANCE             VARCHAR(32),                -- Stored as '1,234.56' — needs cleaning + CAST
    CREDIT_LIMIT        VARCHAR(32),                -- Same issue as BALANCE
    INTEREST_RATE       VARCHAR(16),                -- Stored as '3.50%' — needs stripping
    RISK_SCORE          VARCHAR(4),                 -- '1' to '10' as string
    OPENED_DATE         VARCHAR(32),                -- 'DD-MON-YYYY' format e.g. '15-JAN-2020'
    LAST_ACTIVITY_DATE  VARCHAR(32),
    BRANCH_CODE         VARCHAR(16),
    RELATIONSHIP_MGR    VARCHAR(128),               -- PII — name of relationship manager
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO FINANCE_DB.RAW.ACCOUNTS_RAW
    (ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, ACCOUNT_STATUS, BALANCE,
     CREDIT_LIMIT, INTEREST_RATE, RISK_SCORE, OPENED_DATE,
     LAST_ACTIVITY_DATE, BRANCH_CODE, RELATIONSHIP_MGR)
VALUES
    ('ACC-4532-7891-0023', 'CUST-1001', 'CHECKING',    'A', '12,450.75',  NULL,         '0.50%',  '2',  '15-JAN-2020', '25-JAN-2024', 'NYC-001', 'James Patterson'),
    ('ACC-9921-3344-5567', 'CUST-1002', 'SAVINGS',     'A', '87,320.00',  NULL,         '2.75%',  '1',  '03-MAR-2018', '23-JAN-2024', 'LA-003',  'Maria Gonzalez'),
    ('ACC-1122-8876-9900', 'CUST-1003', 'INVESTMENT',  'A', '245,800.50', NULL,         '5.20%',  '3',  '22-JUL-2015', '20-JAN-2024', 'NYC-002', 'Robert Chen'),
    ('ACC-7743-2211-8854', 'CUST-1004', 'CHECKING',    'A', '3,200.00',   NULL,         '0.50%',  '4',  '10-SEP-2021', '22-JAN-2024', 'LON-001', 'Sarah Williams'),
    ('ACC-5566-9977-3312', 'CUST-1005', 'INVESTMENT',  'A', '1,250,000.00', NULL,       '6.10%',  '1',  '01-JAN-2010', '25-JAN-2024', 'SF-002',  'Michael Torres'),
    ('ACC-3388-1100-2244', 'CUST-1006', 'CHECKING',    'S', '0.00',       NULL,         '0.50%',  '8',  '14-FEB-2022', '19-JAN-2024', 'HOU-001', 'Lisa Anderson'),
    ('ACC-2211-5544-7788', 'CUST-1007', 'CREDIT',      'A', '-4,200.00',  '10,000.00',  '19.99%', '5',  '30-NOV-2019', '24-JAN-2024', 'CHI-001', 'David Kim'),
    ('ACC-8877-6655-4433', 'CUST-1008', 'SAVINGS',     'D', '125.00',     NULL,         '2.75%',  '6',  '05-AUG-2017', '01-OCT-2023', 'BOS-001', 'Emily Brown'),
    ('ACC-6644-3322-1100', 'CUST-1009', 'CREDIT',      'A', '-890.50',    '5,000.00',   '24.99%', '7',  '17-APR-2023', '25-JAN-2024', 'MIA-001', 'Carlos Rivera'),
    ('ACC-9988-7766-5544', 'CUST-1010', 'CHECKING',    'C', '0.00',       NULL,         '0.00%',  '9',  '28-JUN-2016', '15-NOV-2023', 'NYC-003', 'Jennifer White'),
    ('ACC-1234-5678-9012', 'CUST-1011', 'SAVINGS',     'A', '45,670.25',  NULL,         '2.75%',  '2',  '12-DEC-2020', '24-JAN-2024', 'DAL-001', 'Thomas Harris'),
    ('ACC-3456-7890-1234', 'CUST-1012', 'INVESTMENT',  'A', '789,100.00', NULL,         '5.80%',  '1',  '09-OCT-2012', '22-JAN-2024', 'SF-001',  'Amanda Clark');


-- -----------------------------------------------------------------------------
-- 2C. FRAUD_FLAGS_RAW
-- Simulates a real-time fraud detection system CDC feed.
-- Problems baked in:
--   - IS_DELETED flag for soft deletes (needs MERGE with DELETE branch)
--   - FRAUD_SCORE is 1-100 integer but stored as VARCHAR
--   - DETECTION_TIMESTAMP in epoch milliseconds (needs conversion)
--   - RULES_TRIGGERED is a comma-separated string (needs parsing)
--   - SEVERITY_CODE 1-5 integer needs banding to LOW/MEDIUM/HIGH/CRITICAL
--   - Duplicate TRANSACTION_IDs (CDC updates — dedup on MERGE_KEY)
-- Strategy: CDC / MERGE on TRANSACTION_ID
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE FINANCE_DB.RAW.FRAUD_FLAGS_RAW (
    FLAG_ID                 VARCHAR(64)     NOT NULL,
    TRANSACTION_ID          VARCHAR(64)     NOT NULL,   -- Merge key
    ACCOUNT_ID              VARCHAR(32),
    FRAUD_SCORE             VARCHAR(8),                 -- '0' to '100' as string
    SEVERITY_CODE           VARCHAR(4),                 -- '1'=Low,'2'=Med,'3'=High,'4'=Critical,'5'=Blocked
    DETECTION_TIMESTAMP_MS  NUMBER(18,0),               -- Epoch milliseconds
    RULES_TRIGGERED         VARCHAR(512),               -- Comma-separated: 'RULE_042,RULE_017,RULE_099'
    ANALYST_REVIEWED        VARCHAR(8),                 -- 'TRUE','FALSE' as string
    REVIEW_OUTCOME          VARCHAR(32),                -- 'CONFIRMED','FALSE_POSITIVE','PENDING'
    IS_DELETED              BOOLEAN         DEFAULT FALSE,
    CREATED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO FINANCE_DB.RAW.FRAUD_FLAGS_RAW
    (FLAG_ID, TRANSACTION_ID, ACCOUNT_ID, FRAUD_SCORE, SEVERITY_CODE,
     DETECTION_TIMESTAMP_MS, RULES_TRIGGERED, ANALYST_REVIEWED,
     REVIEW_OUTCOME, IS_DELETED, CREATED_AT, UPDATED_AT)
VALUES
    ('FLAG-001', 'TXN-010', 'ACC-5566-9977-3312', '87', '3',
     1706094000000, 'RULE_042,RULE_017', 'TRUE',  'CONFIRMED',      FALSE, '2024-01-24 10:00:00'::TIMESTAMP_NTZ, '2024-01-24 12:00:00'::TIMESTAMP_NTZ),

    ('FLAG-002', 'TXN-012', 'ACC-7743-2211-8854', '95', '4',
     1706180400000, 'RULE_099,RULE_042,RULE_055', 'TRUE',  'CONFIRMED', FALSE, '2024-01-25 10:00:00'::TIMESTAMP_NTZ, '2024-01-25 14:00:00'::TIMESTAMP_NTZ),

    ('FLAG-003', 'TXN-004', 'ACC-4532-7891-0023', '45', '2',
     1705521600000, 'RULE_017', 'FALSE', 'PENDING',         FALSE, '2024-01-17 17:00:00'::TIMESTAMP_NTZ, '2024-01-17 17:00:00'::TIMESTAMP_NTZ),

    ('FLAG-004', 'TXN-007', 'ACC-9921-3344-5567', '72', '3',
     1705694400000, 'RULE_042,RULE_077', 'TRUE',  'FALSE_POSITIVE',  FALSE, '2024-01-19 21:00:00'::TIMESTAMP_NTZ, '2024-01-20 09:00:00'::TIMESTAMP_NTZ),

    ('FLAG-005', 'TXN-015', 'ACC-5566-9977-3312', '91', '4',
     1706180400000, 'RULE_099,RULE_042', 'FALSE', 'PENDING',         FALSE, '2024-01-25 12:00:00'::TIMESTAMP_NTZ, '2024-01-25 12:00:00'::TIMESTAMP_NTZ),

    -- CDC update: FLAG-003 reviewed and resolved
    ('FLAG-003', 'TXN-004', 'ACC-4532-7891-0023', '45', '2',
     1705521600000, 'RULE_017', 'TRUE',  'FALSE_POSITIVE',  FALSE, '2024-01-17 17:00:00'::TIMESTAMP_NTZ, '2024-01-21 10:00:00'::TIMESTAMP_NTZ),

    -- CDC delete: FLAG-004 retracted (false positive — remove from clean table)
    ('FLAG-004', 'TXN-007', 'ACC-9921-3344-5567', '72', '3',
     1705694400000, 'RULE_042,RULE_077', 'TRUE',  'FALSE_POSITIVE',  TRUE,  '2024-01-19 21:00:00'::TIMESTAMP_NTZ, '2024-01-22 08:00:00'::TIMESTAMP_NTZ),

    ('FLAG-006', 'TXN-013', 'ACC-9921-3344-5567', '33', '1',
     1706180400000, 'RULE_011', 'FALSE', 'PENDING',         FALSE, '2024-01-23 18:00:00'::TIMESTAMP_NTZ, '2024-01-23 18:00:00'::TIMESTAMP_NTZ),

    ('FLAG-007', 'TXN-005', 'ACC-7743-2211-8854', '61', '3',
     1705521600000, 'RULE_055,RULE_042', 'TRUE',  'CONFIRMED',       FALSE, '2024-01-18 12:00:00'::TIMESTAMP_NTZ, '2024-01-19 09:00:00'::TIMESTAMP_NTZ),

    ('FLAG-008', 'TXN-002', 'ACC-9921-3344-5567', '22', '1',
     1705435200000, 'RULE_011', 'TRUE',  'FALSE_POSITIVE',  FALSE, '2024-01-16 15:00:00'::TIMESTAMP_NTZ, '2024-01-17 10:00:00'::TIMESTAMP_NTZ);


-- =============================================================================
-- SECTION 3: CLEAN TARGET TABLES
-- These are the analytics-ready tables the agent will load INTO.
-- Schema is clean, properly typed, with business-friendly column names.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3A. TRANSACTIONS_CLEAN
-- Target for TRANSACTIONS_RAW incremental pipeline.
-- All types correct, PII masked, JSON parsed, filters applied.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE FINANCE_DB.CLEAN.TRANSACTIONS_CLEAN (
    TRANSACTION_ID      VARCHAR(64)     NOT NULL PRIMARY KEY,
    CUSTOMER_ID         VARCHAR(32),                -- Renamed from ACCOUNT_NUMBER (masked)
    ACCOUNT_NUMBER_HASH VARCHAR(128),               -- SHA2 masked PII
    TX_DATE             DATE,                       -- Cast from VARCHAR
    TX_AMOUNT           NUMBER(18, 2),              -- Cast + COALESCE(0)
    CURRENCY_CODE       VARCHAR(8),
    MERCHANT_NAME       VARCHAR(256),               -- Parsed from TX_PAYLOAD JSON
    MERCHANT_CATEGORY   VARCHAR(128),               -- Parsed from TX_PAYLOAD JSON
    MERCHANT_CITY       VARCHAR(128),               -- Parsed from TX_PAYLOAD JSON
    MERCHANT_COUNTRY    VARCHAR(8),                 -- Parsed from TX_PAYLOAD JSON
    PAYMENT_METHOD      VARCHAR(64),                -- Parsed from TX_PAYLOAD JSON
    STATUS              VARCHAR(32),
    SOURCE_SYSTEM       VARCHAR(64),
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- -----------------------------------------------------------------------------
-- 3B. ACCOUNTS_CLEAN
-- Target for ACCOUNTS_RAW full load pipeline.
-- All types correct, status codes decoded, PII removed, balances numeric.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE FINANCE_DB.CLEAN.ACCOUNTS_CLEAN (
    ACCOUNT_ID          VARCHAR(32)     NOT NULL PRIMARY KEY,
    CUSTOMER_ID         VARCHAR(32),
    ACCOUNT_TYPE        VARCHAR(32),
    ACCOUNT_STATUS      VARCHAR(32),                -- Decoded: 'Active','Suspended','Closed','Dormant'
    BALANCE             NUMBER(18, 2),              -- Cast from '1,234.56' string
    CREDIT_LIMIT        NUMBER(18, 2),              -- Cast from string, NULL for non-credit accounts
    INTEREST_RATE_PCT   NUMBER(8, 4),               -- Cast from '3.50%' → 3.5
    RISK_SCORE          NUMBER(4, 0),               -- Cast from string
    RISK_BAND           VARCHAR(16),                -- Derived: 'LOW','MEDIUM','HIGH','CRITICAL'
    OPENED_DATE         DATE,                       -- Cast from 'DD-MON-YYYY'
    LAST_ACTIVITY_DATE  DATE,
    BRANCH_CODE         VARCHAR(16),
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    -- RELATIONSHIP_MGR dropped — PII not needed in clean layer
);


-- -----------------------------------------------------------------------------
-- 3C. FRAUD_FLAGS_CLEAN
-- Target for FRAUD_FLAGS_RAW CDC pipeline.
-- Severity decoded, score cast, timestamp converted, rules parsed.
-- Soft-deleted rows are removed via MERGE DELETE branch.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE FINANCE_DB.CLEAN.FRAUD_FLAGS_CLEAN (
    TRANSACTION_ID          VARCHAR(64)     NOT NULL PRIMARY KEY,  -- Merge key
    ACCOUNT_ID              VARCHAR(32),
    FRAUD_SCORE             NUMBER(5, 2),            -- Cast from string
    SEVERITY_CODE           NUMBER(2, 0),            -- Cast from string
    SEVERITY_LABEL          VARCHAR(16),             -- Decoded: 'LOW','MEDIUM','HIGH','CRITICAL','BLOCKED'
    DETECTION_TIMESTAMP     TIMESTAMP_NTZ,           -- Converted from epoch ms
    RULES_TRIGGERED         VARCHAR(512),            -- Kept as-is (array parsing optional)
    RULE_COUNT              NUMBER(4, 0),            -- Derived: count of rules triggered
    ANALYST_REVIEWED        BOOLEAN,                 -- Cast from 'TRUE'/'FALSE' string
    REVIEW_OUTCOME          VARCHAR(32),
    LOADED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    -- IS_DELETED not stored — rows are physically removed via MERGE DELETE
);


-- =============================================================================
-- SECTION 4: GRANT ACCESS TO ETL ROLES
-- =============================================================================
USE ROLE USERADMIN;
CREATE ROLE ETL_AGENT_ROLE;
-- Agent role needs REFERENCES (for GET_DDL) + SELECT on source tables
GRANT USAGE  ON DATABASE FINANCE_DB                        TO ROLE ETL_AGENT_ROLE;
GRANT USAGE  ON SCHEMA   FINANCE_DB.RAW                    TO ROLE ETL_AGENT_ROLE;
GRANT USAGE  ON SCHEMA   FINANCE_DB.CLEAN                  TO ROLE ETL_AGENT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA FINANCE_DB.RAW        TO ROLE ETL_AGENT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA FINANCE_DB.CLEAN      TO ROLE ETL_AGENT_ROLE;

-- Agent role needs INSERT + UPDATE on clean target tables (for task execution)
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCE_DB.CLEAN TO ROLE ETL_AGENT_ROLE;
USE ROLE USERADMIN;
CREATE ROLE ETL_UI_ROLE;
-- UI role needs SELECT on clean tables (for queue display)
GRANT USAGE  ON DATABASE FINANCE_DB                        TO ROLE ETL_UI_ROLE;
GRANT USAGE  ON SCHEMA   FINANCE_DB.RAW                    TO ROLE ETL_UI_ROLE;
GRANT USAGE  ON SCHEMA   FINANCE_DB.CLEAN                  TO ROLE ETL_UI_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA FINANCE_DB.RAW        TO ROLE ETL_UI_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA FINANCE_DB.CLEAN      TO ROLE ETL_UI_ROLE;


-- =============================================================================
-- SECTION 5: VERIFICATION
-- Run these to confirm data loaded correctly before testing the agent.
-- =============================================================================

-- Row counts
SELECT 'TRANSACTIONS_RAW'  AS table_name, COUNT(*) AS row_count FROM FINANCE_DB.RAW.TRANSACTIONS_RAW
UNION ALL
SELECT 'ACCOUNTS_RAW',       COUNT(*) FROM FINANCE_DB.RAW.ACCOUNTS_RAW
UNION ALL
SELECT 'FRAUD_FLAGS_RAW',    COUNT(*) FROM FINANCE_DB.RAW.FRAUD_FLAGS_RAW
UNION ALL
SELECT 'TRANSACTIONS_CLEAN', COUNT(*) FROM FINANCE_DB.CLEAN.TRANSACTIONS_CLEAN
UNION ALL
SELECT 'ACCOUNTS_CLEAN',     COUNT(*) FROM FINANCE_DB.CLEAN.ACCOUNTS_CLEAN
UNION ALL
SELECT 'FRAUD_FLAGS_CLEAN',  COUNT(*) FROM FINANCE_DB.CLEAN.FRAUD_FLAGS_CLEAN;

-- Expected: RAW tables have data, CLEAN tables are empty (agent loads them)

-- Spot check: verify JSON payload parses correctly
SELECT
    TRANSACTION_ID,
    TX_PAYLOAD:merchant_name::STRING    AS merchant_name,
    TX_PAYLOAD:merchant_category::STRING AS merchant_category,
    TX_PAYLOAD:payment_method::STRING   AS payment_method
FROM FINANCE_DB.RAW.TRANSACTIONS_RAW
LIMIT 5;

-- Spot check: verify CDC duplicate rows exist for FRAUD_FLAGS_RAW
SELECT TRANSACTION_ID, COUNT(*) AS versions
FROM FINANCE_DB.RAW.FRAUD_FLAGS_RAW
GROUP BY TRANSACTION_ID
HAVING COUNT(*) > 1;
-- Expected: TXN-004 and TXN-007 appear twice (update + delete CDC events)

-- Spot check: verify ACCOUNTS_RAW messy types
SELECT ACCOUNT_ID, BALANCE, INTEREST_RATE, RISK_SCORE, OPENED_DATE
FROM FINANCE_DB.RAW.ACCOUNTS_RAW
LIMIT 5;
-- Expected: BALANCE as '12,450.75', INTEREST_RATE as '0.50%', RISK_SCORE as '2'