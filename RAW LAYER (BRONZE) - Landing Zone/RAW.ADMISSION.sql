
-- ---------- RAW ADMISSION ----------
CREATE OR REPLACE TABLE HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION (
ADMISSION_ID VARCHAR(50),
PATIENT_ID VARCHAR(50),
DOCTOR_ID VARCHAR(50),
ADMISSION_DATE VARCHAR(50),
DISCHARGE_DATE VARCHAR(50),
ADMISSION_TYPE VARCHAR(50), -- EMERGENCY/ELECTIVE/URGENT
WARD_TYPE VARCHAR(50), -- GENERAL/SEMI-PRIVATE/PRIVATE/ICU
ROOM_NUMBER VARCHAR(20),
BED_NUMBER VARCHAR(20),
ADMITTING_DIAGNOSIS VARCHAR(1000),
DISCHARGE_DIAGNOSIS VARCHAR(1000),
DISCHARGE_STATUS VARCHAR(50), -- RECOVERED/TRANSFERRED/DECEASED/LAMA
ATTENDING_DOCTOR_ID VARCHAR(50),
STATUS VARCHAR(20),
MODIFIED_DATE VARCHAR(50),
-- RAW Metadata
_RAW_FILE_NAME VARCHAR(500),
_RAW_FILE_ROW_NUMBER NUMBER,
_RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
_RAW_BATCH_ID VARCHAR(50),
_RAW_SOURCE_SYSTEM VARCHAR(50) DEFAULT 'S3_HOSPITAL',
_RAW_CHECKSUM VARCHAR(64)
);

SELECT * FROM HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION;

LIST @HOSPITAL_ANALYTICS.RAW.STG_S3_HOSPITAL/RAW-ADMISSION/;


SHOW PROCEDURES;









-- ================================================================
-- ================================================================
--    🔥 MAIN STORED PROCEDURE: SP_LOAD_S3_TO_RAW_ADMISSION
-- ================================================================
-- ================================================================
/*
   ╔═══════════════════════════════════════════════════════════════════╗
   ║  PROCEDURE: SP_LOAD_S3_TO_RAW_ADMISSION                        ║
   ║                                                                  ║
   ║  PURPOSE: Load admission CSV files from S3 into RAW_ADMISSION   ║
   ║                                                                  ║
   ║  PARAMETERS:                                                     ║
   ║    P_SOURCE_SYSTEM  → 'HIS' (Hospital Information System)       ║
   ║    P_YEAR           → '2025'                                     ║
   ║    P_MONTH          → '02'                                       ║
   ║    P_DAY            → '19'                                       ║
   ║                                                                  ║
   ║  S3 PATH BUILT AS:                                               ║
   ║    ADMISSION/{source_system}/year={year}/month={month}/day={day}/║
   ║    Example: ADMISSION/HIS/year=2025/month=02/day=19/            ║
   ║                                                                  ║
   ║  EXECUTION FLOW:                                                 ║
   ║    STEP 1: Validate parameters                                   ║
   ║    STEP 2: Build S3 path & check files exist                    ║
   ║    STEP 3: COPY INTO raw table                                   ║
   ║    STEP 4: Log file-level results                               ║
   ║    STEP 5: Update metadata columns (batch_id, checksum, etc.)   ║
   ║    STEP 6: Run data quality checks                              ║
   ║    STEP 7: Update pipeline config with last run info            ║
   ║    STEP 8: Final status update                                   ║
   ╚═══════════════════════════════════════════════════════════════════╝
*/

CALL HOSPITAL_ANALYTICS.RAW.SP_LOAD_S3_TO_RAW_ADMISSION('source_system', '2026', '02', '19');




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

    v_files      NUMBER DEFAULT 0;
    v_loaded     NUMBER DEFAULT 0;
    v_failed     NUMBER DEFAULT 0;
    v_rows_ok    NUMBER DEFAULT 0;
    v_rows_err   NUMBER DEFAULT 0;
    v_dq_count   NUMBER DEFAULT 0;

    v_start      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_status     VARCHAR DEFAULT 'STARTED';
    v_error      VARCHAR DEFAULT '';
    v_error_type VARCHAR DEFAULT 'SQL_ERROR';
    v_warning    VARCHAR DEFAULT '';
    v_sql        VARCHAR DEFAULT '';
BEGIN
    /* ── Normalize month/day to 2-digit strings ── */
    v_month2 := LPAD(TO_VARCHAR(TO_NUMBER(P_MONTH)), 2, '0');
    v_day2   := LPAD(TO_VARCHAR(TO_NUMBER(P_DAY)),   2, '0');

    v_batch_id   := 'BATCH_ADM_'
                    || P_SOURCE || '_'
                    || P_YEAR   || v_month2 || v_day2
                    || '_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'HH24MISS');

    v_s3_path    := 'RAW-ADMISSION/' || P_SOURCE
                    || '/year='  || P_YEAR
                    || '/month=' || v_month2
                    || '/day='   || v_day2 || '/';

    v_stage_path := '@HOSPITAL_ANALYTICS.RAW.STG_S3_HOSPITAL/' || v_s3_path;

    /* ══════════════════════════════════════
       AUDIT: pipeline start
       ══════════════════════════════════════ */
    INSERT INTO HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
        (EXECUTION_ID, PIPELINE_NAME, LAYER_NAME,
         SOURCE_TABLE, TARGET_TABLE,
         EXECUTION_STATUS, BATCH_ID, START_TIMESTAMP)
    SELECT
        :v_exec_id,
        'S3_TO_RAW_ADMISSION',
        'RAW',
        'S3:' || :v_s3_path,
        'RAW.RAW_ADMISSION',
        'STARTED',
        :v_batch_id,
        :v_start;

    BEGIN
        /* ══════════════════════════════════════
           STEP 1: VALIDATE PARAMETERS
           ══════════════════════════════════════ */
        IF (    P_SOURCE IS NULL
             OR TRIM(P_SOURCE) = ''
             OR TRY_TO_NUMBER(P_YEAR)  IS NULL
             OR LENGTH(P_YEAR) <> 4
             OR TRY_TO_NUMBER(P_MONTH) IS NULL
             OR TRY_TO_NUMBER(P_MONTH) NOT BETWEEN 1 AND 12
             OR TRY_TO_NUMBER(P_DAY)   IS NULL
             OR TRY_TO_NUMBER(P_DAY)   NOT BETWEEN 1 AND 31 )
        THEN
            v_error := 'Invalid params → SRC=' || COALESCE(P_SOURCE, 'NULL')
                       || ', Y=' || COALESCE(P_YEAR,  'NULL')
                       || ', M=' || COALESCE(P_MONTH, 'NULL')
                       || ', D=' || COALESCE(P_DAY,   'NULL');

            INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
                (EXECUTION_ID, EXCEPTION_TYPE, EXCEPTION_SOURCE,
                 EXCEPTION_MESSAGE, CONTEXT_INFO)
            SELECT
                :v_exec_id,
                'VALIDATION_ERROR',
                'SP_LOAD_S3_TO_RAW_ADMISSION',
                :v_error,
                OBJECT_CONSTRUCT(
                    'source', :P_SOURCE,
                    'year',   :P_YEAR,
                    'month',  :P_MONTH,
                    'day',    :P_DAY
                );

            UPDATE HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
            SET EXECUTION_STATUS = 'FAILED',
                ERROR_MESSAGE    = :v_error,
                END_TIMESTAMP    = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP())
            WHERE EXECUTION_ID = :v_exec_id;

            RETURN OBJECT_CONSTRUCT(
                'status',       'FAILED',
                'error',        :v_error,
                'execution_id', :v_exec_id
            );
        END IF;

        /* ══════════════════════════════════════
           STEP 2: COPY INTO RAW_ADMISSION
           ══════════════════════════════════════ */
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
FROM
(
  SELECT
    $1,  $2,  $3,  $4,  $5,
    $6,  $7,  $8,  $9,  $10,
    $11, $12, $13, $14, $15,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    CURRENT_TIMESTAMP(),
    ''' || :v_batch_id                      || ''',
    ''' || REPLACE(:P_SOURCE, '''', '''''') || ''',
    NULL
  FROM ' || :v_stage_path || '
)
FILE_FORMAT = (FORMAT_NAME = ''HOSPITAL_ANALYTICS.RAW.CSV_FORMAT'')
PATTERN     = ''.*\\.csv''
ON_ERROR    = CONTINUE
PURGE       = FALSE
FORCE       = FALSE';

        EXECUTE IMMEDIATE :v_sql;

        /* ══════════════════════════════════════════════════════════════
           STEP 2A: DERIVE FILE STATS FROM RAW_ADMISSION DIRECTLY
           ══════════════════════════════════════════════════════════════ */
        CREATE OR REPLACE TEMP TABLE TMP_COPY_RESULT AS
        SELECT
            _RAW_FILE_NAME            AS FILE_PATH,
            COUNT(*)                  AS ROWS_LOADED,
            MAX(_RAW_FILE_ROW_NUMBER) AS MAX_ROW_NUMBER
        FROM HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
        WHERE _RAW_BATCH_ID = :v_batch_id
        GROUP BY _RAW_FILE_NAME;

        SELECT COUNT(*) INTO :v_files FROM TMP_COPY_RESULT;

        /* ══════════════════════════════════════
           STEP 2B: NO FILES FOUND AT PATH
           ══════════════════════════════════════ */
        IF (v_files = 0) THEN

            INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
                (EXECUTION_ID, EXCEPTION_TYPE, EXCEPTION_SOURCE,
                 EXCEPTION_MESSAGE, CONTEXT_INFO)
            SELECT
                :v_exec_id,
                'FILE_NOT_FOUND',
                'SP_LOAD_S3_TO_RAW_ADMISSION',
                'No CSV files loaded from: ' || :v_s3_path,
                OBJECT_CONSTRUCT(
                    'stage_path', :v_stage_path,
                    's3_path',    :v_s3_path
                );

            UPDATE HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
            SET EXECUTION_STATUS = 'SUCCESS',
                ERROR_MESSAGE    = 'No files found at: ' || :v_s3_path,
                END_TIMESTAMP    = CURRENT_TIMESTAMP(),
                DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP())
            WHERE EXECUTION_ID = :v_exec_id;

            RETURN OBJECT_CONSTRUCT(
                'status',     'SUCCESS',
                'message',    'No files found',
                's3_path',    :v_s3_path,
                'stage_path', :v_stage_path
            );
        END IF;

        /* ══════════════════════════════════════════════════════════════
           STEP 3: BACKFILL CHECKSUM
           ──────────────────────────────────────────────────────────────
           FIX: SHA2() already returns a hex string in Snowflake.
                TO_HEX() does NOT exist. Removed it.
           ══════════════════════════════════════════════════════════════ */
        UPDATE HOSPITAL_ANALYTICS.RAW.RAW_ADMISSION
        SET _RAW_CHECKSUM = SHA2(
            CONCAT_WS('||',
                COALESCE(TO_VARCHAR(ADMISSION_ID),        ''),
                COALESCE(TO_VARCHAR(PATIENT_ID),          ''),
                COALESCE(TO_VARCHAR(DOCTOR_ID),           ''),
                COALESCE(TO_VARCHAR(ADMISSION_DATE),      ''),
                COALESCE(TO_VARCHAR(DISCHARGE_DATE),      ''),
                COALESCE(TO_VARCHAR(ADMISSION_TYPE),      ''),
                COALESCE(TO_VARCHAR(WARD_TYPE),           ''),
                COALESCE(TO_VARCHAR(ROOM_NUMBER),         ''),
                COALESCE(TO_VARCHAR(BED_NUMBER),          ''),
                COALESCE(TO_VARCHAR(ADMITTING_DIAGNOSIS), ''),
                COALESCE(TO_VARCHAR(DISCHARGE_DIAGNOSIS), ''),
                COALESCE(TO_VARCHAR(DISCHARGE_STATUS),    ''),
                COALESCE(TO_VARCHAR(ATTENDING_DOCTOR_ID), ''),
                COALESCE(TO_VARCHAR(STATUS),              ''),
                COALESCE(TO_VARCHAR(MODIFIED_DATE),       '')
            ), 256)
        WHERE _RAW_BATCH_ID  = :v_batch_id
          AND _RAW_CHECKSUM IS NULL;

        /* ══════════════════════════════════════
           STEP 4: FILE INGESTION LOG
           ══════════════════════════════════════ */
        INSERT INTO HOSPITAL_ANALYTICS.AUDIT.FILE_INGESTION_LOG
            (EXECUTION_ID, FILE_NAME, FILE_PATH, FILE_SIZE_BYTES,
             FILE_FORMAT, SOURCE_SYSTEM, TABLE_NAME, INGESTION_STATUS,
             ROWS_PARSED, ROWS_LOADED, ROWS_ERRORS,
             FIRST_ERROR, FIRST_ERROR_LINE)
        SELECT
            :v_exec_id,
            SPLIT_PART(FILE_PATH, '/', -1)  AS FILE_NAME,
            FILE_PATH,
            0                               AS FILE_SIZE_BYTES,
            'CSV',
            :P_SOURCE,
            'RAW_ADMISSION',
            'SUCCESS'                       AS INGESTION_STATUS,
            ROWS_LOADED                     AS ROWS_PARSED,
            ROWS_LOADED,
            0                               AS ROWS_ERRORS,
            NULL                            AS FIRST_ERROR,
            NULL                            AS FIRST_ERROR_LINE
        FROM TMP_COPY_RESULT;

        SELECT
            COUNT(*),
            COUNT(CASE WHEN INGESTION_STATUS = 'SUCCESS' THEN 1 END),
            COUNT(CASE WHEN INGESTION_STATUS = 'FAILED'  THEN 1 END),
            COALESCE(SUM(ROWS_LOADED), 0),
            COALESCE(SUM(ROWS_ERRORS), 0)
        INTO :v_files, :v_loaded, :v_failed, :v_rows_ok, :v_rows_err
        FROM HOSPITAL_ANALYTICS.AUDIT.FILE_INGESTION_LOG
        WHERE EXECUTION_ID = :v_exec_id;

        /* ══════════════════════════════════════
           STEP 5: LOG COPY ERRORS
           ══════════════════════════════════════ */
        INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
            (EXECUTION_ID, EXCEPTION_TYPE, EXCEPTION_SOURCE,
             EXCEPTION_MESSAGE, CONTEXT_INFO)
        SELECT
            :v_exec_id,
            'COPY_ERROR',
            'SP_LOAD_S3_TO_RAW_ADMISSION',
            'File ' || FILE_NAME
                || ' → ' || COALESCE(FIRST_ERROR, 'Unknown error'),
            OBJECT_CONSTRUCT(
                'file',   FILE_PATH,
                'parsed', ROWS_PARSED,
                'loaded', ROWS_LOADED,
                'errors', ROWS_ERRORS
            )
        FROM HOSPITAL_ANALYTICS.AUDIT.FILE_INGESTION_LOG
        WHERE EXECUTION_ID    = :v_exec_id
          AND INGESTION_STATUS IN ('FAILED', 'PARTIAL');

        /* ══════════════════════════════════════
           STEP 6: DATA QUALITY CHECKS
           ══════════════════════════════════════ */
        INSERT INTO HOSPITAL_ANALYTICS.AUDIT.DATA_QUALITY_ERRORS
            (EXECUTION_ID, BATCH_ID,
             SOURCE_TABLE, TARGET_TABLE, LAYER_NAME,
             ERROR_CATEGORY, ERROR_SEVERITY,
             COLUMN_NAME, RULE_NAME,
             ERROR_DESCRIPTION, EXPECTED_VALUE, ACTUAL_VALUE,
             RECORD_IDENTIFIER, REJECTED_RECORD)
        SELECT
            :v_exec_id,
            :v_batch_id,
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
        WHERE _RAW_BATCH_ID = :v_batch_id
          AND (ADMISSION_ID IS NULL OR TRIM(ADMISSION_ID) = '');

        SELECT COUNT(*) INTO :v_dq_count
        FROM HOSPITAL_ANALYTICS.AUDIT.DATA_QUALITY_ERRORS
        WHERE EXECUTION_ID = :v_exec_id;

        IF (v_dq_count > 0) THEN
            v_warning := v_dq_count || ' DQ issues found';
        END IF;

        /* ══════════════════════════════════════
           STEP 7: DETERMINE FINAL STATUS
           ══════════════════════════════════════ */
        IF (v_failed = v_files AND v_files > 0) THEN
            v_status := 'FAILED';
        ELSEIF (v_failed > 0) THEN
            v_status := 'PARTIAL';
        ELSE
            v_status := 'SUCCESS';
        END IF;

        UPDATE HOSPITAL_ANALYTICS.CONFIG.PIPELINE_CONFIG
        SET UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE TABLE_NAME = 'RAW_ADMISSION'
          AND IS_ACTIVE  = TRUE;

        UPDATE HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG
        SET EXECUTION_STATUS = :v_status,
            RECORDS_READ     = :v_rows_ok + :v_rows_err,
            RECORDS_INSERTED = :v_rows_ok,
            RECORDS_REJECTED = :v_rows_err,
            ERROR_MESSAGE    = :v_warning,
            END_TIMESTAMP    = CURRENT_TIMESTAMP(),
            DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP())
        WHERE EXECUTION_ID = :v_exec_id;

        RETURN OBJECT_CONSTRUCT(
            'status',       :v_status,
            'execution_id', :v_exec_id,
            'batch_id',     :v_batch_id,
            's3_path',      :v_s3_path,
            'files_total',  :v_files,
            'files_loaded', :v_loaded,
            'files_failed', :v_failed,
            'rows_loaded',  :v_rows_ok,
            'rows_errored', :v_rows_err,
            'dq_issues',    :v_dq_count
        );

    EXCEPTION
        WHEN OTHER THEN
            v_error := SQLERRM;

            v_error_type :=
                CASE
                    WHEN v_error ILIKE '%access%denied%' THEN 'PERMISSION'
                    WHEN v_error ILIKE '%timeout%'        THEN 'TIMEOUT'
                    WHEN v_error ILIKE '%warehouse%'      THEN 'RESOURCE'
                    ELSE                                       'SQL_ERROR'
                END;

            /* ── Always log to EXCEPTION_LOG ── */
            INSERT INTO HOSPITAL_ANALYTICS.AUDIT.EXCEPTION_LOG
                (EXECUTION_ID, EXCEPTION_TYPE, EXCEPTION_SOURCE,
                 EXCEPTION_MESSAGE, SQL_STATEMENT, CONTEXT_INFO)
            SELECT
                :v_exec_id,
                :v_error_type,
                'SP_LOAD_S3_TO_RAW_ADMISSION',
                :v_error,
                :v_sql,
                OBJECT_CONSTRUCT(
                    'source',  :P_SOURCE,
                    'year',    :P_YEAR,
                    'month',   :P_MONTH,
                    'day',     :P_DAY,
                    's3_path', :v_s3_path,
                    'batch',   :v_batch_id
                );

            /* ══════════════════════════════════════════════════════════
               FIX: Use MERGE instead of UPDATE so that even if the
               initial INSERT into PIPELINE_EXECUTION_LOG never
               committed, we still create a FAILED record.
               ══════════════════════════════════════════════════════════ */
            MERGE INTO HOSPITAL_ANALYTICS.AUDIT.PIPELINE_EXECUTION_LOG AS tgt
            USING (SELECT :v_exec_id AS EXECUTION_ID) AS src
               ON tgt.EXECUTION_ID = src.EXECUTION_ID

            WHEN MATCHED THEN UPDATE SET
                tgt.EXECUTION_STATUS = 'FAILED',
                tgt.ERROR_MESSAGE    = :v_error,
                tgt.END_TIMESTAMP    = CURRENT_TIMESTAMP(),
                tgt.DURATION_SECONDS = TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP())

            WHEN NOT MATCHED THEN INSERT
                (EXECUTION_ID, PIPELINE_NAME, LAYER_NAME,
                 SOURCE_TABLE, TARGET_TABLE,
                 EXECUTION_STATUS, BATCH_ID,
                 START_TIMESTAMP, END_TIMESTAMP, DURATION_SECONDS,
                 ERROR_MESSAGE)
            VALUES
                (:v_exec_id,
                 'S3_TO_RAW_ADMISSION',
                 'RAW',
                 'S3:' || :v_s3_path,
                 'RAW.RAW_ADMISSION',
                 'FAILED',
                 :v_batch_id,
                 :v_start,
                 CURRENT_TIMESTAMP(),
                 TIMESTAMPDIFF(SECOND, :v_start, CURRENT_TIMESTAMP()),
                 :v_error);

            RETURN OBJECT_CONSTRUCT(
                'status',       'FAILED',
                'execution_id', :v_exec_id,
                'batch_id',     :v_batch_id,
                'error',        :v_error,
                'error_type',   :v_error_type
            );
    END;
END;
$$;