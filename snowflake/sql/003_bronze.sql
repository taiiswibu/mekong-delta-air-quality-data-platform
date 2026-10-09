USE WAREHOUSE AQ_WH;
USE DATABASE AQ_WAREHOUSE;
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT AQ_JSON_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = TRUE;

CREATE OR REPLACE FILE FORMAT AQ_CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null', 'NA', 'N/A')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE STAGE IF NOT EXISTS AQ_INTERNAL_STAGE;

CREATE TABLE IF NOT EXISTS API_RAW (
    event_id STRING NOT NULL,
    queried_city STRING NOT NULL,
    raw_payload VARIANT NOT NULL,
    source_event_at TIMESTAMP_TZ,
    produced_at TIMESTAMP_TZ,
    kafka_partition INT,
    kafka_offset NUMBER,
    ingested_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (queried_city, TO_DATE(ingested_at));

CREATE TABLE IF NOT EXISTS HISTORICAL_CSV (
    station_id STRING,
    aqi_index STRING,
    location STRING,
    station_name STRING,
    url STRING,
    dominant_pollutant STRING,
    co STRING,
    dew STRING,
    humidity STRING,
    no2 STRING,
    o3 STRING,
    pressure STRING,
    pm10 STRING,
    pm25 STRING,
    so2 STRING,
    temperature STRING,
    wind STRING,
    data_time_s STRING,
    data_time_tz STRING,
    status STRING,
    alert_level STRING,
    source_file STRING,
    ingested_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
