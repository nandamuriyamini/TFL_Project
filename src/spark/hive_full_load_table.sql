-- Hive external table on top of the TFL Spark Full Load Parquet output
-- Partitioned by lineName and arrival_date for fast filtered queries

CREATE DATABASE IF NOT EXISTS yamini_tfl_proj;

DROP TABLE IF EXISTS yamini_tfl_proj.tfl_full_load;

CREATE EXTERNAL TABLE yamini_tfl_proj.tfl_full_load (
    id              STRING,
    vehicleId       STRING,
    stationName     STRING,
    platformName    STRING,
    expectedArrival STRING,
    timeToStation   INT,
    currentLocation STRING,
    direction       STRING,
    destinationName STRING,
    timestamp       STRING,
    kafka_offset    BIGINT,
    kafka_ingest_time TIMESTAMP,
    loaded_at       TIMESTAMP
)
PARTITIONED BY (
    lineName     STRING,
    arrival_date DATE
)
STORED AS PARQUET
LOCATION '/tmp/yamini/tfl_full_load/output';

-- Auto-discover all partitions written by Spark
MSCK REPAIR TABLE yamini_tfl_proj.tfl_full_load;

-- Total record count
SELECT COUNT(*) AS total_records FROM yamini_tfl_proj.tfl_full_load;

-- Records per tube line
SELECT lineName, COUNT(*) AS record_count
FROM   yamini_tfl_proj.tfl_full_load
GROUP  BY lineName
ORDER  BY record_count DESC;
