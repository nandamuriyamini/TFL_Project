-- Hive external table on top of Spark Structured Streaming Parquet output

CREATE DATABASE IF NOT EXISTS yamini_tfl_proj;

DROP TABLE IF EXISTS yamini_tfl_proj.tfl_spark_arrivals;

CREATE EXTERNAL TABLE yamini_tfl_proj.tfl_spark_arrivals (
    id              STRING,
    vehicleId       STRING,
    stationName     STRING,
    lineName        STRING,
    platformName    STRING,
    expectedArrival STRING,
    timeToStation   INT,
    currentLocation STRING,
    direction       STRING,
    destinationName STRING,
    arr_timestamp   STRING,
    ingested_at     TIMESTAMP
)
STORED AS PARQUET
LOCATION '/tmp/yamini/tfl_spark_streaming/output';

SELECT COUNT(*) AS total_records FROM yamini_tfl_proj.tfl_spark_arrivals;
