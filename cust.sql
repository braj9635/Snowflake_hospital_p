
============================================================================================================

   create OR REPLACE storage integration STORAGE_S3_INTEGRATION
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::278891233718:role/iam_snoflake_role'
    enabled = true
    storage_allowed_locations = ( 's3://new-hospital-db' )
    -- storage_blocked_locations = ( 's3://<location1>', 's3://<location2>' )
    -- comment = '<comment>'
    ; 

       DESCRIBE storage 