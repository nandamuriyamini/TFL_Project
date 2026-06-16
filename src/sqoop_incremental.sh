#!/bin/bash

YEAR=${1:-2020}
PG_CONN="jdbc:postgresql://13.42.152.118:5432/testdb"
PG_USER="admin"
PG_PASS="admin123"
INC="/tmp/yamini/tfl_project1/incremental"

echo "============================================="
echo " INCREMENTAL SQOOP IMPORT  --  year=${YEAR}"
echo "============================================="

# ── Dimension tables (small, re-import fresh as reference) ──────────

for tbl in dim_date dim_lines dim_networks dim_stations fact_station_lines; do
  echo "--- Importing ${tbl} ---"
  sqoop import \
    -D mapreduce.framework.name=local \
    --connect  "${PG_CONN}" \
    --username "${PG_USER}" \
    --password "${PG_PASS}" \
    --table    "${tbl}" \
    --target-dir "${INC}/${tbl}" \
    --num-mappers 1 \
    --fields-terminated-by ',' \
    --delete-target-dir
done

# ── Fact table – only records for the target year ───────────────────

echo "--- Importing fact_passenger_entry_exit for year=${YEAR} ---"
sqoop import \
  -D mapreduce.framework.name=local \
  --connect  "${PG_CONN}" \
  --username "${PG_USER}" \
  --password "${PG_PASS}" \
  --query    "SELECT fpe.entry_exit_id, fpe.station_id, fpe.date_id,
                     fpe.total_entry_exit, fpe.estimated_entries,
                     fpe.estimated_exits, fpe.record_type,
                     fpe.data_source, fpe.created_at
              FROM   fact_passenger_entry_exit fpe
              JOIN   dim_date dd ON fpe.date_id = dd.date_id
              WHERE  dd.year = ${YEAR}
              AND    \$CONDITIONS" \
  --target-dir "${INC}/fact_passenger_entry_exit" \
  --num-mappers 1 \
  --fields-terminated-by ',' \
  --delete-target-dir

echo "============================================="
echo " INCREMENTAL SQOOP COMPLETE  --  year=${YEAR}"
echo "============================================="
