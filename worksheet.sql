-- create cloud storage integration
CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::number:role/mysnowflakerole' -- removed for security measures
  STORAGE_ALLOWED_LOCATIONS = ('*');

-- get integration details
DESC INTEGRATION s3_int;

-- create custom file format
-- (won't work correctly without one, because name of airbnb listing sometimes contains ',', which causes failure in pipe)
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 0
  NULL_IF = ('');

-- create external stage for bucket referencing, data will be queued there before loading to table
CREATE OR REPLACE STAGE mystage1
  URL = 's3://snowflake-test-tania'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = csv_format;


-- create pipe with specified file format to move data from stage to raw data table
CREATE PIPE nyc_airbnb.public.mypipe
  AUTO_INGEST = TRUE
  AS
    COPY INTO nyc_airbnb.public.airbnb_raw
      FROM @nyc_airbnb.public.mystage1
      FILE_FORMAT = (FORMAT_NAME = 'csv_format');

-- check if permissions were set up correctly
SELECT
  SYSTEM$VALIDATE_STORAGE_INTEGRATION(
    'S3_INT',
    's3://snowflake-test-tania',
    'validate_all.txt', 'all');


-- set up ownership to use pipes
CREATE OR REPLACE ROLE snowpipe_role;
GRANT USAGE ON database nyc_airbnb TO ROLE snowpipe_role;
GRANT USAGE ON schema nyc_airbnb.public TO ROLE snowpipe_role;
GRANT insert, SELECT ON nyc_airbnb.public.airbnb_raw TO ROLE snowpipe_role;
GRANT USAGE ON stage nyc_airbnb.public.mystage1 TO ROLE snowpipe_role;
ALTER PIPE mypipe SET PIPE_EXECUTION_PAUSED = TRUE;
GRANT OWNERSHIP ON PIPE nyc_airbnb.public.mypipe TO ROLE snowpipe_role;
GRANT ROLE snowpipe_role TO USER TANYABILANIUK;

-- show ingestion results for past 24 hours
SELECT * FROM table(information_schema.copy_history(table_name=>'nyc_airbnb.public.airbnb_raw', start_time=> dateadd('hour', -24, current_timestamp())));

-- create stream for transforming raw data
CREATE OR REPLACE STREAM airbnb_transform_stream
ON TABLE nyc_airbnb.public.airbnb_raw;

-- create table for transformed data
CREATE OR REPLACE TABLE NYC_AIRBNB.PUBLIC.AIRBNB_TRANSFORMED (
	ID NUMBER(10,0) NOT NULL,
	NAME VARCHAR(255),
	HOST_ID NUMBER(10,0),
	HOST_NAME VARCHAR(255),
	NEIGHBOURHOOD_GROUP VARCHAR(255),
	NEIGHBOURHOOD VARCHAR(255),
	LATITUDE NUMBER(9,6),
	LONGITUDE NUMBER(9,6),
	ROOM_TYPE VARCHAR(50),
	PRICE NUMBER(10,0),
	MINIMUM_NIGHTS NUMBER(5,0),
	NUMBER_OF_REVIEWS NUMBER(5,0),
	LAST_REVIEW DATE,
	REVIEWS_PER_MONTH NUMBER(5,3),
	CALCULATED_HOST_LISTINGS_COUNT NUMBER(10,3),
	AVAILABILITY_365 NUMBER(4,0),
	primary key (ID)
);

-- create task for data transformation
CREATE OR REPLACE TASK airbnb_transform_task
WAREHOUSE = TEST_WH
SCHEDULE = '1 DAY'
AS
INSERT INTO nyc_airbnb.public.airbnb_transformed
SELECT
    ID,
    NAME,
    HOST_ID,
	HOST_NAME,
	NEIGHBOURHOOD_GROUP,
	NEIGHBOURHOOD,
	LATITUDE,
	LONGITUDE,
	ROOM_TYPE,
	PRICE,
	MINIMUM_NIGHTS,
	NUMBER_OF_REVIEWS,
	DATE(COALESCE(LAST_REVIEW, '2024-09-16')),
	COALESCE(REVIEWS_PER_MONTH, 0),
	CALCULATED_HOST_LISTINGS_COUNT,
	AVAILABILITY_365
FROM nyc_airbnb.public.airbnb_transform_stream
WHERE METADATA$ACTION = 'INSERT'
    AND PRICE > 0
    AND LATITUDE IS NOT NULL
    AND LONGITUDE IS NOT NULL;

-- start transformation task and check the status of the task
ALTER TASK airbnb_transform_task RESUME;
SHOW TASKS LIKE 'airbnb_transform_task';

-- verify data transformation
SELECT * FROM nyc_airbnb.public.airbnb_transformed WHERE PRICE = 0; -- should return 0 rows
SELECT * FROM nyc_airbnb.public.airbnb_transformed WHERE LATITUDE IS NULL OR LONGITUDE IS NULL; -- should return 0 rows
SELECT * FROM nyc_airbnb.public.airbnb_transformed WHERE LAST_REVIEW = '2024-09-16';

-- check if there is unprocessed data in stream
SELECT SYSTEM$STREAM_HAS_DATA('nyc_airbnb.public.airbnb_transform_stream');

-- check stream details
SHOW STREAMS LIKE 'airbnb_transform_stream';

-- view task execution history
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;

-- execute transformation task without waiting for scheduled time
EXECUTE TASK airbnb_transform_task;

-- compare transformed data from 40 minutes ago and now
SELECT count(*) FROM nyc_airbnb.public.airbnb_transformed AT(OFFSET => -60*40);
SELECT count(*) FROM nyc_airbnb.public.airbnb_transformed;