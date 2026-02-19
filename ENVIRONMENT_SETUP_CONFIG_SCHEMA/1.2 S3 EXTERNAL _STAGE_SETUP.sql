

-- ============================================================
-- 1.2 S3 EXTERNAL STORAGE INTEGRATION SETUP
-- ============================================================

CREATE STORAGE INTEGRATION S3_HOSPITAL_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::278891233718:role/IAM_SNOW_ROLE'
    STORAGE_ALLOWED_LOCATIONS = ('s3://new-hospital-db/')
    COMMENT = 'AWS S3 POLICY AND USER';

    DESC STORAGE INTEGRATION S3_HOSPITAL_INT;

-- ============================================================
--  S3 EXTERNAL STAGE 
-- ============================================================
    
CREATE OR REPLACE STAGE HOSPITAL_ANALYTICS.RAW.STG_S3_HOSPITAL
    STORAGE_INTEGRATION = S3_HOSPITAL_INT
    URL = 's3://new-hospital-db/'
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
                   SKIP_HEADER = 1 NULL_IF = ('NULL', 'null', '', 'N/A')
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE);

LIST@RAW.STG_S3_HOSPITAL;  -- Fetch All Files in s3 bucket



-- ============================================================
--  File Formats
-- ============================================================

-- CSV

CREATE OR REPLACE FILE FORMAT HOSPITAL_ANALYTICS.RAW.CSV_FORMAT
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL','null')
EMPTY_FIELD_AS_NULL = TRUE;

SHOW FILE FORMATS IN SCHEMA HOSPITAL_ANALYTICS.RAW;



    
--JSON
    
CREATE OR REPLACE FILE FORMAT HOSPITAL_ANALYTICS.RAW.FF_JSON_HOSPITAL
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = FALSE;

--Parquet

CREATE OR REPLACE FILE FORMAT HOSPITAL_ANALYTICS.RAW.FF_PARQUET_HOSPITAL
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE;

    