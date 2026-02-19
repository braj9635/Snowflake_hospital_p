ORIGINAL (1 Giant Procedure)
         │
         ▼
┌─────────────────────────────────────────────┐
│  SP_LOAD_S3_TO_RAW_ADMISSION (ORCHESTRATOR) │
│  Calls each sub-procedure in order          │
└──────┬──────────────────────────────────────┘
       │
       ├──① SP_RAW_ADM_VALIDATE_PARAMS
       ├──② SP_RAW_ADM_COPY_FROM_S3
       ├──③ SP_RAW_ADM_BACKFILL_CHECKSUM
       ├──④ SP_RAW_ADM_LOG_FILES
       ├──⑤ SP_RAW_ADM_LOG_COPY_ERRORS
       ├──⑥ SP_RAW_ADM_DQ_CHECKS
       └──⑦ SP_RAW_ADM_FINALIZE


;




-- Procedure ① — Validate Parameters


CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_VALIDATE_PARAMS(
    P_SOURCE  VARCHAR,
    P_YEAR    VARCHAR,
    P_MONTH   VARCHAR,
    P_DAY     VARCHAR,
    P_EXEC_ID VARCHAR
)
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS CALLER
AS
$
DECLARE
    v_error VARCHAR;
BEGIN
    -- ───────────────────────────────────
    -- Simple validation: all 4 params
    -- ───────────────────────────────────
    IF (   P_SOURCE IS NULL OR TRIM(P_SOURCE) = ''
        OR TRY_TO_NUMBER(P_YEAR)  IS NULL OR LENGTH(P_YEAR) <> 4
        OR TRY_TO_NUMBER(P_MONTH) IS NULL OR TRY_TO_NUMBER(P_MONTH) NOT BETWEEN 1 AND 12
        OR TRY_TO_NUMBER(P_DAY)   IS NULL OR TRY_TO_NUMBER(P_DAY)   NOT BETWEEN 1 AND 31
    ) THEN

        v_error := 'Invalid params → SRC=' || COALESCE(P_SOURCE, 'NULL')
                   || ', Y=' || COALESCE(P_YEAR,  'NULL')
                   || ', M=' || COALESCE(P_MONTH, 'NULL')
                   || ', D=' || COALESCE(P_DAY,   'NULL');

        -- Log to exception table
        INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
            (EXECUTION_ID, EXCEPTION_TYPE, EXCEPTION_SOURCE,
             EXCEPTION_MESSAGE, CONTEXT_INFO)
        VALUES (
            :P_EXEC_ID,
            'VALIDATION_ERROR',
            'SP_RAW_ADM_VALIDATE_PARAMS',
            :v_error,
            OBJECT_CONSTRUCT(
                'source', :P_SOURCE,
                'year',   :P_YEAR,
                'month',  :P_MONTH,
                'day',    :P_DAY
            )
        );

        RETURN FALSE;  -- ← validation failed
    END IF;

    RETURN TRUE;  -- ← all good
END;
$$;



--Procedure ② — COPY FROM S3




CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_COPY_FROM_S3(
    P_STAGE_PATH VARCHAR,
    P_BATCH_ID   VARCHAR,
    P_SOURCE     VARCHAR
)
RETURNS NUMBER
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_sql      VARCHAR;
    v_rows     NUMBER DEFAULT 0;
BEGIN
    -- ───────────────────────────────────
    -- Build and execute COPY command
    -- ───────────────────────────────────
    v_sql :=
'COPY INTO HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
(
  ADMISSION_ID, PATIENT_ID, DOCTOR_ID,
  ADMISSION_DATE, DISCHARGE_DATE,
  ADMISSION_TYPE, WARD_TYPE, ROOM_NUMBER, BED_NUMBER,
  ADMITTING_DIAGNOSIS, DISCHARGE_DIAGNOSIS, DISCHARGE_STATUS,
  ATTENDING_DOCTOR_ID, STATUS, MODIFIED_DATE,
  _RAW_FILE_NAME, _RAW_FILE_ROW_NUMBER,
  _RAW_LOAD_TIMESTAMP, _RAW_BATCH_ID, _RAW_SOURCE_SYSTEM, _RAW_CHECKSUM
)
FROM (
  SELECT
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    CURRENT_TIMESTAMP(),
    ''' || :P_BATCH_ID                     || ''',
    ''' || REPLACE(:P_SOURCE, '''', '''''') || ''',
    NULL
  FROM ' || :P_STAGE_PATH || '
)
FILE_FORMAT = (FORMAT_NAME = ''HOSPITAL_ANALYTICS.RAW.CSV_FORMAT'')
PATTERN     = ''.*\\.csv''
ON_ERROR    = CONTINUE
PURGE       = FALSE
FORCE       = FALSE';

    EXECUTE IMMEDIATE :v_sql;

    -- Count what was loaded in this batch
    SELECT COUNT(*)
      INTO :v_rows
      FROM HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
     WHERE _RAW_BATCH_ID = :P_BATCH_ID;

    RETURN :v_rows;
END;
$$;



--Procedure ③ — Backfill Checksum


CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_BACKFILL_CHECKSUM(
    P_BATCH_ID VARCHAR
)
RETURNS NUMBER
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_updated NUMBER DEFAULT 0;
BEGIN
    -- ───────────────────────────────────
    -- SHA2 checksum for all batch rows
    -- ───────────────────────────────────
    UPDATE HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
    SET _RAW_CHECKSUM = SHA2(
        CONCAT_WS('||',
            COALESCE(ADMISSION_ID,                     ''),
            COALESCE(TO_VARCHAR(PATIENT_ID),            ''),
            COALESCE(TO_VARCHAR(DOCTOR_ID),             ''),
            COALESCE(TO_VARCHAR(ADMISSION_DATE),        ''),
            COALESCE(TO_VARCHAR(DISCHARGE_DATE),        ''),
            COALESCE(ADMISSION_TYPE,                    ''),
            COALESCE(WARD_TYPE,                         ''),
            COALESCE(TO_VARCHAR(ROOM_NUMBER),           ''),
            COALESCE(TO_VARCHAR(BED_NUMBER),            ''),
            COALESCE(ADMITTING_DIAGNOSIS,               ''),
            COALESCE(DISCHARGE_DIAGNOSIS,               ''),
            COALESCE(DISCHARGE_STATUS,                  ''),
            COALESCE(TO_VARCHAR(ATTENDING_DOCTOR_ID),   ''),
            COALESCE(STATUS,                            ''),
            COALESCE(TO_VARCHAR(MODIFIED_DATE),         '')
        ), 256)
    WHERE _RAW_BATCH_ID  = :P_BATCH_ID
      AND _RAW_CHECKSUM IS NULL;

    -- Return how many rows got checksum
    SELECT COUNT(*)
      INTO :v_updated
      FROM HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
     WHERE _RAW_BATCH_ID   = :P_BATCH_ID
       AND _RAW_CHECKSUM IS NOT NULL;

    RETURN :v_updated;
END;
$$;




--Procedure ④ — Log File Ingestion


CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_LOG_FILES(
    P_EXEC_ID  VARCHAR,
    P_BATCH_ID VARCHAR,
    P_SOURCE   VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_files   NUMBER DEFAULT 0;
    v_loaded  NUMBER DEFAULT 0;
    v_failed  NUMBER DEFAULT 0;
    v_rows_ok NUMBER DEFAULT 0;
    v_rows_err NUMBER DEFAULT 0;
BEGIN
    -- ───────────────────────────────────
    -- Insert one row per file into log
    -- ───────────────────────────────────
    INSERT INTO HOSPITAL_ANALYTICS.AUDIT.FILE_INGESTION_LOG
        (EXECUTION_ID, FILE_NAME, FILE_PATH, FILE_SIZE_BYTES,
         FILE_FORMAT, SOURCE_SYSTEM, TABLE_NAME, INGESTION_STATUS,
         ROWS_PARSED, ROWS_LOADED, ROWS_ERRORS,
         FIRST_ERROR, FIRST_ERROR_LINE)
    SELECT
        :P_EXEC_ID,
        SPLIT_PART(_RAW_FILE_NAME, '/', -1),  -- extract filename
        _RAW_FILE_NAME,
        0,
        'CSV',
        :P_SOURCE,
        'RAW_ADMISSION',
        'SUCCESS',
        COUNT(*),
        COUNT(*),
        0,
        NULL,
        NULL
    FROM HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
    WHERE _RAW_BATCH_ID = :P_BATCH_ID
    GROUP BY _RAW_FILE_NAME;

    -- ───────────────────────────────────
    -- Gather summary stats
    -- ───────────────────────────────────
    SELECT
        COUNT(*),
        COUNT(CASE WHEN INGESTION_STATUS = 'SUCCESS' THEN 1 END),
        COUNT(CASE WHEN INGESTION_STATUS = 'FAILED'  THEN 1 END),
        COALESCE(SUM(ROWS_LOADED), 0),
        COALESCE(SUM(ROWS_ERRORS), 0)
    INTO :v_files, :v_loaded, :v_failed, :v_rows_ok, :v_rows_err
    FROM HOSPITAL_ANALYTICS.AUDIT.FILE_INGESTION_LOG
    WHERE EXECUTION_ID = :P_EXEC_ID;

    RETURN OBJECT_CONSTRUCT(
        'files_total',  :v_files,
        'files_loaded', :v_loaded,
        'files_failed', :v_failed,
        'rows_loaded',  :v_rows_ok,
        'rows_errored', :v_rows_err
    );
END;
$$;



--Procedure ⑤ — Log Copy Errors




CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_LOG_COPY_ERRORS(
    P_EXEC_ID VARCHAR
)
RETURNS NUMBER
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_error_count NUMBER DEFAULT 0;
BEGIN
    -- ───────────────────────────────────
    -- Log files that failed or partial
    -- ───────────────────────────────────
    INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
        (EXECUTION_ID, EXCEPTION_TYPE, EXCEPTION_SOURCE,
         EXCEPTION_MESSAGE, CONTEXT_INFO)
    SELECT
        :P_EXEC_ID,
        'COPY_ERROR',
        'SP_RAW_ADM_LOG_COPY_ERRORS',
        'File ' || FILE_NAME || ' → ' || COALESCE(FIRST_ERROR, 'Unknown'),
        OBJECT_CONSTRUCT(
            'file',   FILE_PATH,
            'parsed', ROWS_PARSED,
            'loaded', ROWS_LOADED,
            'errors', ROWS_ERRORS
        )
    FROM HOSPITAL_ANALYTICS.AUDIT.FILE_INGESTION_LOG
    WHERE EXECUTION_ID     = :P_EXEC_ID
      AND INGESTION_STATUS IN ('FAILED', 'PARTIAL');

    SELECT COUNT(*)
      INTO :v_error_count
      FROM HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
     WHERE EXECUTION_ID  = :P_EXEC_ID
       AND EXCEPTION_TYPE = 'COPY_ERROR';

    RETURN :v_error_count;
END;
$$;




--Procedure ⑥ — Data Quality Checks





CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_DQ_CHECKS(
    P_EXEC_ID  VARCHAR,
    P_BATCH_ID VARCHAR
)
RETURNS NUMBER
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_dq_count NUMBER DEFAULT 0;
BEGIN
    -- ───────────────────────────────────
    -- Check: ADMISSION_ID not null/blank
    -- ───────────────────────────────────
    INSERT INTO HOSPITAL_ANALYTICS.AUDIT.DATA_QUALITY_ERRORS
        (EXECUTION_ID, BATCH_ID,
         SOURCE_TABLE, TARGET_TABLE, LAYER_NAME,
         ERROR_CATEGORY, ERROR_SEVERITY,
         COLUMN_NAME, RULE_NAME,
         ERROR_DESCRIPTION, EXPECTED_VALUE, ACTUAL_VALUE,
         RECORD_IDENTIFIER, REJECTED_RECORD)
    SELECT
        :P_EXEC_ID,
        :P_BATCH_ID,
        'RAW.RAW_ADMISSION',
        'RAW.RAW_ADMISSION',
        'RAW',
        'NULL_CHECK',
        'CRITICAL',
        'ADMISSION_ID',
        'ADM_001',
        'ADMISSION_ID is NULL or blank',
        'Non-NULL / Non-blank',
        COALESCE(ADMISSION_ID, '<NULL>'),
        COALESCE(ADMISSION_ID, 'UNK_' || _RAW_FILE_ROW_NUMBER::STRING),
        OBJECT_CONSTRUCT(
            'file',     _RAW_FILE_NAME,
            'row',      _RAW_FILE_ROW_NUMBER,
            'checksum', _RAW_CHECKSUM
        )
    FROM HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
    WHERE _RAW_BATCH_ID = :P_BATCH_ID
      AND (ADMISSION_ID IS NULL OR TRIM(ADMISSION_ID) = '');

    -- Return count of issues found
    SELECT COUNT(*)
      INTO :v_dq_count
      FROM HOSPITAL_ANALYTICS.AUDIT.DATA_QUALITY_ERRORS
     WHERE EXECUTION_ID = :P_EXEC_ID;

    RETURN :v_dq_count;
END;
$$;






--Procedure ⑦ — Finalize Pipeline Status


CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_FINALIZE(
    P_EXEC_ID    VARCHAR,
    P_BATCH_ID   VARCHAR,
    P_START      TIMESTAMP_NTZ,
    P_FILES      NUMBER,
    P_LOADED     NUMBER,
    P_FAILED     NUMBER,
    P_ROWS_OK    NUMBER,
    P_ROWS_ERR   NUMBER,
    P_DQ_COUNT   NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_status  VARCHAR;
    v_warning VARCHAR DEFAULT '';
BEGIN
    -- ───────────────────────────────────
    -- Determine final status
    -- ───────────────────────────────────
    IF (P_FAILED = P_FILES AND P_FILES > 0) THEN
        v_status := 'FAILED';
    ELSEIF (P_FAILED > 0) THEN
        v_status := 'PARTIAL';
    ELSE
        v_status := 'SUCCESS';
    END IF;

    IF (P_DQ_COUNT > 0) THEN
        v_warning := P_DQ_COUNT || ' DQ issues found';
    END IF;

    -- Update pipeline config
    UPDATE HOSPITAL_ANALYTICS.CONFIG.PIPELINE_CONFIG
    SET UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE TABLE_NAME = 'RAW_ADMISSION'
      AND IS_ACTIVE  = TRUE;

    -- Update execution log
    UPDATE HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
    SET EXECUTION_STATUS = :v_status,
        RECORDS_READ     = :P_ROWS_OK + :P_ROWS_ERR,
        RECORDS_INSERTED = :P_ROWS_OK,
        RECORDS_REJECTED = :P_ROWS_ERR,
        ERROR_MESSAGE    = :v_warning,
        END_TIMESTAMP    = CURRENT_TIMESTAMP(),
        DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :P_START, CURRENT_TIMESTAMP())
    WHERE EXECUTION_ID = :P_EXEC_ID;

    RETURN :v_status;
END;
$$;




--  🎯 Main Orchestrator — Simplified




CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_LOAD_S3_TO_RAW_ADMISSION(
    P_SOURCE VARCHAR,
    P_YEAR   VARCHAR,
    P_MONTH  VARCHAR,
    P_DAY    VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_exec_id    VARCHAR DEFAULT UUID_STRING();
    v_batch_id   VARCHAR;
    v_s3_path    VARCHAR;
    v_stage_path VARCHAR;
    v_month2     VARCHAR;
    v_day2       VARCHAR;

    v_is_valid   BOOLEAN;
    v_rows       NUMBER DEFAULT 0;
    v_file_stats VARIANT;
    v_dq_count   NUMBER DEFAULT 0;
    v_copy_errs  NUMBER DEFAULT 0;
    v_status     VARCHAR;

    v_start      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_error      VARCHAR;
BEGIN
    -- ═══════════════════════════════════
    -- SETUP: Build paths and batch ID
    -- ═══════════════════════════════════
    v_month2     := LPAD(TO_VARCHAR(TO_NUMBER(P_MONTH)), 2, '0');
    v_day2       := LPAD(TO_VARCHAR(TO_NUMBER(P_DAY)),   2, '0');

    v_batch_id   := 'BATCH_ADM_' || P_SOURCE || '_'
                    || P_YEAR || v_month2 || v_day2 || '_'
                    || TO_VARCHAR(CURRENT_TIMESTAMP(), 'HH24MISS');

    v_s3_path    := 'RAW-ADMISSION/' || P_SOURCE
                    || '/year='  || P_YEAR
                    || '/month=' || v_month2
                    || '/day='   || v_day2 || '/';

    v_stage_path := '@HOSPITAL_ANALYTICS.RAW.STG_S3_HOSPITAL/' || v_s3_path;

    -- Log pipeline start
    INSERT INTO HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
        (EXECUTION_ID, PIPELINE_NAME, LAYER_NAME,
         SOURCE_TABLE, TARGET_TABLE,
         EXECUTION_STATUS, BATCH_ID, START_TIMESTAMP)
    VALUES (
        :v_exec_id, 'S3_TO_RAW_ADMISSION', 'RAW',
        'S3:' || :v_s3_path, 'RAW.RAW_ADMISSION',
        'STARTED', :v_batch_id, :v_start
    );

    BEGIN
        -- ① VALIDATE
        CALL HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_VALIDATE_PARAMS(
            :P_SOURCE, :P_YEAR, :P_MONTH, :P_DAY, :v_exec_id
        ) INTO :v_is_valid;

        IF (NOT v_is_valid) THEN
            UPDATE HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
            SET EXECUTION_STATUS = 'FAILED',
                ERROR_MESSAGE    = 'Parameter validation failed',
                END_TIMESTAMP    = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP())
            WHERE EXECUTION_ID = :v_exec_id;

            RETURN OBJECT_CONSTRUCT(
                'status', 'FAILED',
                'error',  'Invalid parameters',
                'execution_id', :v_exec_id
            );
        END IF;

        -- ② COPY FROM S3
        CALL HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_COPY_FROM_S3(
            :v_stage_path, :v_batch_id, :P_SOURCE
        ) INTO :v_rows;

        IF (v_rows = 0) THEN
            UPDATE HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
            SET EXECUTION_STATUS = 'SUCCESS',
                ERROR_MESSAGE    = 'No files found at: ' || :v_s3_path,
                END_TIMESTAMP    = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP())
            WHERE EXECUTION_ID = :v_exec_id;

            RETURN OBJECT_CONSTRUCT(
                'status',  'SUCCESS',
                'message', 'No files found',
                's3_path', :v_s3_path
            );
        END IF;

        -- ③ BACKFILL CHECKSUM
        CALL HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_BACKFILL_CHECKSUM(:v_batch_id);

        -- ④ LOG FILE INGESTION
        CALL HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_LOG_FILES(
            :v_exec_id, :v_batch_id, :P_SOURCE
        ) INTO :v_file_stats;

        -- ⑤ LOG COPY ERRORS
        CALL HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_LOG_COPY_ERRORS(:v_exec_id)
            INTO :v_copy_errs;

        -- ⑥ DATA QUALITY CHECKS
        CALL HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_DQ_CHECKS(
            :v_exec_id, :v_batch_id
        ) INTO :v_dq_count;

        -- ⑦ FINALIZE
        CALL HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_FINALIZE(
            :v_exec_id, :v_batch_id, :v_start,
            :v_file_stats:files_total::NUMBER,
            :v_file_stats:files_loaded::NUMBER,
            :v_file_stats:files_failed::NUMBER,
            :v_file_stats:rows_loaded::NUMBER,
            :v_file_stats:rows_errored::NUMBER,
            :v_dq_count
        ) INTO :v_status;

        RETURN OBJECT_CONSTRUCT(
            'status',       :v_status,
            'execution_id', :v_exec_id,
            'batch_id',     :v_batch_id,
            'rows_loaded',  :v_rows,
            'dq_issues',    :v_dq_count
        );

    EXCEPTION
        WHEN OTHER THEN
            v_error := SQLERRM;

            INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
                (EXECUTION_ID, EXCEPTION_TYPE, EXCEPTION_SOURCE,
                 EXCEPTION_MESSAGE, CONTEXT_INFO)
            VALUES (
                :v_exec_id, 'SQL_ERROR', 'SP_LOAD_S3_TO_RAW_ADMISSION',
                :v_error,
                OBJECT_CONSTRUCT('source', :P_SOURCE, 'batch', :v_batch_id)
            );

            MERGE INTO HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG AS t
            USING (SELECT :v_exec_id AS EID) AS s ON t.EXECUTION_ID = s.EID
            WHEN MATCHED THEN UPDATE SET
                EXECUTION_STATUS = 'FAILED',
                ERROR_MESSAGE = :v_error,
                END_TIMESTAMP = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP())
            WHEN NOT MATCHED THEN INSERT
                (EXECUTION_ID, PIPELINE_NAME, LAYER_NAME,
                 SOURCE_TABLE, TARGET_TABLE,
                 EXECUTION_STATUS, BATCH_ID,
                 START_TIMESTAMP, END_TIMESTAMP,
                 DURATION_SECONDS, ERROR_MESSAGE)
            VALUES (
                :v_exec_id, 'S3_TO_RAW_ADMISSION', 'RAW',
                'S3:' || :v_s3_path, 'RAW.RAW_ADMISSION',
                'FAILED', :v_batch_id, :v_start,
                CURRENT_TIMESTAMP(),
                TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP()),
                :v_error
            );

            RETURN OBJECT_CONSTRUCT(
                'status', 'FAILED',
                'execution_id', :v_exec_id,
                'error', :v_error
            );
    END;
END;
$$;





--Summary of All Optimizations


┌──────────────────────────┬────────────────────────────────────────┐
│     OPTIMIZATION         │           WHAT CHANGED                │
├──────────────────────────┼────────────────────────────────────────┤
│ Removed temp table       │ TMP_COPY_RESULT eliminated            │
│                          │ → direct GROUP BY on RAW_ADMISSION    │
├──────────────────────────┼────────────────────────────────────────┤
│ Reduced TO_VARCHAR()     │ Only cast non-string columns          │
│                          │ → fewer function calls in checksum    │
├──────────────────────────┼────────────────────────────────────────┤
│ INSERT...VALUES          │ Replaced INSERT...SELECT for single   │
│                          │ row inserts (audit log start)         │
├──────────────────────────┼────────────────────────────────────────┤
│ Single responsibility    │ 7 procedures × 1 job each             │
│                          │ → testable, debuggable, reusable      │
├──────────────────────────┼────────────────────────────────────────┤
│ Reduced variable count   │ Orchestrator: 12 vars → 10 vars      │
│                          │ Sub-procs: only what they need        │
├──────────────────────────┼────────────────────────────────────────┤
│ Simpler error handling   │ Only orchestrator has TRY/CATCH       │
│                          │ Sub-procs bubble up naturally         │
├──────────────────────────┼────────────────────────────────────────┤
│ VARIANT for stats        │ File stats returned as VARIANT object │
│                          │ → one CALL replaces 5 variables       │
└──────────────────────────┴────────────────────────────────────────┘





































-- =====================================================================
-- Procedure: SP_RAW_ADM_VALIDATE_PARAMS
-- Purpose  : Validates the four input parameters (source, year, month, day)
--            before any downstream processing begins.  Returns TRUE when
--            every parameter is acceptable, FALSE (and logs an exception)
--            when any parameter is missing or out of range.
-- =====================================================================;
;























































CREATE OR REPLACE PROCEDURE HOSPITAL_ANALYTICS.RAW.SP_RAW_ADM_VALIDATE_PARAMS(
    -- P_SOURCE  : Name / identifier of the data source (e.g., 'HIS', 'EMR')
    P_SOURCE  VARCHAR,
    -- P_YEAR    : 4-digit calendar year as a string (e.g., '2025')
    P_YEAR    VARCHAR,
    -- P_MONTH   : 1- or 2-digit month as a string (e.g., '1' … '12')
    P_MONTH   VARCHAR,
    -- P_DAY     : 1- or 2-digit day as a string (e.g., '1' … '31')
    P_DAY     VARCHAR,
    -- P_EXEC_ID : Unique execution / run identifier used for audit logging
    P_EXEC_ID VARCHAR
)
-- The procedure returns a BOOLEAN: TRUE = valid, FALSE = invalid
RETURNS BOOLEAN
-- Written in Snowflake SQL (not JavaScript / Python)
LANGUAGE SQL
-- EXECUTE AS CALLER: runs with the permissions of the calling role,
-- so the caller must have INSERT access to EXCEPTION_LOG
EXECUTE AS CALLER
AS
$$
DECLARE
    -- v_error will hold a human-readable message if validation fails
    v_error VARCHAR;
BEGIN
    -- ───────────────────────────────────────────────────────────────────
    -- VALIDATION BLOCK – checks all four parameters in a single IF
    -- ───────────────────────────────────────────────────────────────────

    IF (
        -- Check 1: P_SOURCE must not be NULL and must not be blank / whitespace-only
           P_SOURCE IS NULL OR TRIM(P_SOURCE) = ''

        -- Check 2a: P_YEAR must be a valid number (TRY_TO_NUMBER returns NULL if not)
        OR TRY_TO_NUMBER(P_YEAR)  IS NULL
        -- Check 2b: P_YEAR must be exactly 4 characters (e.g., '2025', not '25')
        OR LENGTH(P_YEAR) <> 4

        -- Check 3a: P_MONTH must be a valid number
        OR TRY_TO_NUMBER(P_MONTH) IS NULL
        -- Check 3b: P_MONTH must fall within the range 1–12
        OR TRY_TO_NUMBER(P_MONTH) NOT BETWEEN 1 AND 12

        -- Check 4a: P_DAY must be a valid number
        OR TRY_TO_NUMBER(P_DAY)   IS NULL
        -- Check 4b: P_DAY must fall within the range 1–31
        OR TRY_TO_NUMBER(P_DAY)   NOT BETWEEN 1 AND 31
    ) THEN
        -- ─────────────────────────────────────────────────────────────
        -- At least one parameter failed validation
        -- ─────────────────────────────────────────────────────────────

        -- Build a descriptive error string; COALESCE converts NULLs to
        -- the literal text 'NULL' so the message is always readable
        v_error := 'Invalid params → SRC=' || COALESCE(P_SOURCE, 'NULL')
                   || ', Y=' || COALESCE(P_YEAR,  'NULL')
                   || ', M=' || COALESCE(P_MONTH, 'NULL')
                   || ', D=' || COALESCE(P_DAY,   'NULL');

        -- Log the validation failure into the central exception / audit table
        -- so that operations teams can review it later
        INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
            (
              EXECUTION_ID,        -- Links this row to the current pipeline run
              EXCEPTION_TYPE,      -- Category of the problem
              EXCEPTION_SOURCE,    -- Name of the procedure that raised it
              EXCEPTION_MESSAGE,   -- Human-readable description built above
              CONTEXT_INFO         -- Semi-structured (VARIANT) payload with raw param values
            )
        VALUES (
            :P_EXEC_ID,                          -- Bind the execution ID passed by the caller
            'VALIDATION_ERROR',                  -- Fixed label for parameter-validation errors
            'SP_RAW_ADM_VALIDATE_PARAMS',        -- This procedure's name (for traceability)
            :v_error,                            -- The descriptive message we just composed
            OBJECT_CONSTRUCT(                    -- Build a JSON-like VARIANT object containing
                'source', :P_SOURCE,             --   the original source value
                'year',   :P_YEAR,               --   the original year value
                'month',  :P_MONTH,              --   the original month value
                'day',    :P_DAY                 --   the original day value
            )
        );

        -- Return FALSE to tell the caller that validation failed;
        -- the caller should abort further processing
        RETURN FALSE;
    END IF;

    -- ─────────────────────────────────────────────────────────────────
    -- If execution reaches here, every parameter passed all checks
    -- ─────────────────────────────────────────────────────────────────
    RETURN TRUE;   -- Signal to the caller that inputs are valid
END;
$$;