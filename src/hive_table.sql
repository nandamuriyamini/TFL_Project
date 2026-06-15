CREATE DATABASE IF NOT EXISTS yamini_tfl_proj;
USE yamini_tfl_proj;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  date_id      INT,
  year         INT,
  quarter      INT,
  month        INT,
  is_annual    BOOLEAN,
  period_name  STRING,
  start_date   DATE,
  end_date     DATE,
  load_ts      TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/yamini/tfl_project1/dim_date'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_lines (
  line_id     INT,
  line_name   STRING,
  line_colour STRING,
  is_active   BOOLEAN,
  created_ts  TIMESTAMP,
  updated_ts  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/yamini/tfl_project1/dim_lines'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_networks (
  network_id   INT,
  network_name STRING,
  network_type STRING,
  created_ts   TIMESTAMP,
  updated_ts   TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/yamini/tfl_project1/dim_networks'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS dim_stations (
  station_id   INT,
  station_code DOUBLE,
  station_name STRING,
  network_id   INT,
  flag_1       BOOLEAN,
  flag_2       BOOLEAN,
  flag_3       BOOLEAN,
  flag_4       BOOLEAN,
  flag_5       BOOLEAN,
  flag_6       BOOLEAN,
  created_ts   TIMESTAMP,
  updated_ts   TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/yamini/tfl_project1/dim_stations'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS fact_station_lines (
  station_line_id INT,
  station_id      INT,
  line_id         INT,
  is_interchange  BOOLEAN,
  effective_from  DATE,
  effective_to    DATE,
  created_at      TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/yamini/tfl_project1/fact_station_lines'
TBLPROPERTIES ('serialization.null.format'='null');

CREATE EXTERNAL TABLE IF NOT EXISTS fact_passenger_entry_exit (
  entry_exit_id     BIGINT,
  station_id        INT,
  date_id           INT,
  total_entry_exit  BIGINT,
  estimated_entries BIGINT,
  estimated_exits   BIGINT,
  record_type       STRING,
  data_source       STRING,
  created_at        TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/yamini/tfl_project1/fact_passenger_entry_exit'
TBLPROPERTIES ('serialization.null.format'='null');

-- Busiest stations per year
SELECT d.year,
       s.station_name,
       SUM(f.total_entry_exit) AS total_passengers
FROM fact_passenger_entry_exit f
JOIN dim_date d
  ON f.date_id = d.date_id
JOIN dim_stations s
  ON f.station_id = s.station_id
GROUP BY d.year, s.station_name
ORDER BY d.year DESC, total_passengers DESC;

-- Passengers by line
SELECT l.line_name,
       SUM(f.total_entry_exit) AS total_passengers
FROM fact_passenger_entry_exit f
JOIN fact_station_lines b
    ON f.station_id = b.station_id
JOIN dim_lines l
    ON b.line_id = l.line_id
GROUP BY l.line_name
ORDER BY total_passengers DESC;

-- Year-over-year total ridership trend
SELECT s.station_name, COUNT(b.line_id) AS num_lines
FROM dim_stations s
JOIN fact_station_lines b ON s.station_id = b.station_id
GROUP BY s.station_name
ORDER BY num_lines DESC;

