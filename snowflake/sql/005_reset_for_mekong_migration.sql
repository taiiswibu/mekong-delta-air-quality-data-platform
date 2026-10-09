-- ONE-TIME MIGRATION RESET
-- Run only when switching this portfolio project from the old Vietnam-wide
-- demo dataset to the Mekong Delta dataset.

USE ROLE SYSADMIN;
USE WAREHOUSE AQ_WH;
USE DATABASE AQ_WAREHOUSE;

-- Raw data from the previous demo geography.
TRUNCATE TABLE BRONZE.API_RAW;
TRUNCATE TABLE BRONZE.HISTORICAL_CSV;

-- Allow the new historical seed file to be loaded.
DELETE FROM CONTROL.LOADED_FILES
WHERE layer = 'bronze_historical';

-- Rebuild dbt-managed analytical schemas from clean Bronze data.
DROP SCHEMA IF EXISTS SILVER CASCADE;
CREATE SCHEMA SILVER;

DROP SCHEMA IF EXISTS GOLD CASCADE;
CREATE SCHEMA GOLD;

-- Optional: uncomment for a clean portfolio monitoring history.
-- TRUNCATE TABLE CONTROL.PIPELINE_RUNS;
-- TRUNCATE TABLE CONTROL.PIPELINE_TASK_EVENTS;
-- TRUNCATE TABLE CONTROL.PIPELINE_METRICS;
-- TRUNCATE TABLE CONTROL.DQ_RESULTS;
-- TRUNCATE TABLE CONTROL.REJECTED_EVENTS;
