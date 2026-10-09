USE WAREHOUSE AQ_WH;
USE DATABASE AQ_WAREHOUSE;
USE SCHEMA CONTROL;

CREATE TABLE IF NOT EXISTS CITY_MAP (
    source_city STRING NOT NULL,
    conformed_city STRING NOT NULL
);

TRUNCATE TABLE CITY_MAP;

INSERT INTO CITY_MAP VALUES
    ('Can Tho', 'can-tho'),
    ('Cần Thơ', 'can-tho'),
    ('can-tho', 'can-tho'),

    ('An Giang', 'an-giang'),
    ('an-giang', 'an-giang'),

    ('Dong Thap', 'dong-thap'),
    ('Đồng Tháp', 'dong-thap'),
    ('dong-thap', 'dong-thap'),

    ('Long An', 'long-an'),
    ('long-an', 'long-an'),

    ('Tien Giang', 'tien-giang'),
    ('Tiền Giang', 'tien-giang'),
    ('tien-giang', 'tien-giang'),

    ('Ben Tre', 'ben-tre'),
    ('Bến Tre', 'ben-tre'),
    ('ben-tre', 'ben-tre'),

    ('Vinh Long', 'vinh-long'),
    ('Vĩnh Long', 'vinh-long'),
    ('vinh-long', 'vinh-long'),

    ('Tra Vinh', 'tra-vinh'),
    ('Trà Vinh', 'tra-vinh'),
    ('tra-vinh', 'tra-vinh'),

    ('Hau Giang', 'hau-giang'),
    ('Hậu Giang', 'hau-giang'),
    ('hau-giang', 'hau-giang'),

    ('Soc Trang', 'soc-trang'),
    ('Sóc Trăng', 'soc-trang'),
    ('soc-trang', 'soc-trang'),

    ('Bac Lieu', 'bac-lieu'),
    ('Bạc Liêu', 'bac-lieu'),
    ('bac-lieu', 'bac-lieu'),

    ('Ca Mau', 'ca-mau'),
    ('Cà Mau', 'ca-mau'),
    ('ca-mau', 'ca-mau'),

    ('Kien Giang', 'kien-giang'),
    ('Kiên Giang', 'kien-giang'),
    ('kien-giang', 'kien-giang');

CREATE TABLE IF NOT EXISTS CITY_REFERENCE (
    city_slug STRING NOT NULL,
    city_name STRING NOT NULL,
    region STRING NOT NULL,
    lat DOUBLE,
    lon DOUBLE
);

TRUNCATE TABLE CITY_REFERENCE;

-- "region" below is an analytical grouping for this portfolio,
-- not an official administrative classification.
INSERT INTO CITY_REFERENCE VALUES
    ('can-tho', 'Can Tho', 'Central Mekong', 10.0452, 105.7469),
    ('an-giang', 'An Giang', 'Upper Mekong', 10.3864, 105.4352),
    ('dong-thap', 'Dong Thap', 'Upper Mekong', 10.4602, 105.6320),
    ('long-an', 'Long An', 'Gateway', 10.5359, 106.4137),
    ('tien-giang', 'Tien Giang', 'Tien River', 10.3600, 106.3600),
    ('ben-tre', 'Ben Tre', 'Coastal', 10.2434, 106.3756),
    ('vinh-long', 'Vinh Long', 'Central Mekong', 10.2537, 105.9722),
    ('tra-vinh', 'Tra Vinh', 'Coastal', 9.9347, 106.3453),
    ('hau-giang', 'Hau Giang', 'Central Mekong', 9.7845, 105.4701),
    ('soc-trang', 'Soc Trang', 'Coastal', 9.6025, 105.9800),
    ('bac-lieu', 'Bac Lieu', 'Coastal', 9.2940, 105.7278),
    ('ca-mau', 'Ca Mau', 'Coastal', 9.1769, 105.1524),
    ('kien-giang', 'Kien Giang', 'Western Coastal', 10.0125, 105.0809);

CREATE TABLE IF NOT EXISTS LOADED_FILES (
    file_name STRING,
    layer STRING,
    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_loaded_file UNIQUE(file_name, layer)
);

CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
    run_id STRING,
    pipeline_name STRING,
    orchestrator_run_id STRING,
    status STRING,
    started_at TIMESTAMP_TZ,
    finished_at TIMESTAMP_TZ,
    silver_rows_before NUMBER,
    processed_rows NUMBER,
    failed_rows NUMBER,
    message STRING
);

ALTER TABLE PIPELINE_RUNS ADD COLUMN IF NOT EXISTS silver_rows_before NUMBER;
ALTER TABLE PIPELINE_RUNS ADD COLUMN IF NOT EXISTS processed_rows NUMBER;
ALTER TABLE PIPELINE_RUNS ADD COLUMN IF NOT EXISTS failed_rows NUMBER;

CREATE TABLE IF NOT EXISTS PIPELINE_TASK_EVENTS (
    run_id STRING,
    task_name STRING,
    status STRING,
    detail VARIANT,
    event_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PIPELINE_METRICS (
    run_id STRING,
    metric_name STRING,
    metric_value DOUBLE,
    metric_text STRING,
    captured_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS REJECTED_EVENTS (
    source STRING,
    raw_payload STRING,
    reason STRING,
    kafka_partition INT,
    kafka_offset NUMBER,
    rejected_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DQ_RESULTS (
    run_id STRING,
    framework STRING,
    check_name STRING,
    severity STRING,
    passed BOOLEAN,
    detail VARIANT,
    checked_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
