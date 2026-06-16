#!/usr/bin/env python3
"""
=============================================================
 FILE: incremental_spark.py
 PROJECT: TFL Data Pipeline - Yamini
 LOAD TYPE: TRULY INCREMENTAL SPARK
=============================================================

PURPOSE:
  Process ONLY new incremental data (e.g. 2020-2021).
  Merge intelligently with existing gold tables.
  Does NOT re-read or re-process the full load data (2017-2019).

TWO MERGE STRATEGIES based on gold table type:

  STRATEGY 1 - APPEND (year-based gold tables):
    gold_passengers_by_year  -> new year rows added, no overlap
    gold_quarterly_trend     -> new quarters added, no overlap
    Guarded by the incremental_watermark table.

  STRATEGY 2 - MERGE (cross-year aggregate gold tables):
    gold_busiest_stations       -> re-sum old gold + new totals
    gold_passengers_by_line     -> re-sum old gold + new totals
    gold_passengers_by_network  -> re-sum old gold + new totals
    gold_interchange_stations   -> refresh from new mapping data
    gold_night_tube_analysis    -> re-sum old gold + new totals

HDFS PATHS:
  Full load raw data  : /tmp/yamini/tfl_project1/
  Incremental raw data: /tmp/yamini/tfl_project1/incremental/
  Gold tables         : /tmp/yamini/tfl_project1/gold/
=============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, desc, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

spark = SparkSession.builder \
    .appName("TFL_Incremental_Spark_Yamini") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================
#  HDFS PATHS
# =============================================================

HDFS_INC  = "/tmp/yamini/tfl_project1/incremental"
HDFS_FULL = "/tmp/yamini/tfl_project1"
GOLD_BASE = "/tmp/yamini/tfl_project1/gold"
HIVE_DB   = "yamini_tfl_proj"

spark.sql(f"USE {HIVE_DB}")

print("=" * 60)
print("TFL TRULY INCREMENTAL SPARK PIPELINE - Yamini")
print("Processes ONLY new data")
print("Merges with existing gold tables")
print("=" * 60)


# =============================================================
#  HELPERS
# =============================================================

def _get_fs():
    sc = spark.sparkContext
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

def _get_path(path):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)

def hdfs_exists(path):
    try:
        return _get_fs().exists(_get_path(path))
    except Exception:
        return False

def hdfs_has_data(path):
    try:
        fs = _get_fs()
        p  = _get_path(path)
        if not fs.exists(p):
            return False
        statuses = fs.listStatus(p)
        return any("part-" in str(s.getPath().getName()) for s in statuses)
    except Exception:
        return False

def read_csv(path, columns):
    return (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(path)
        .toDF(*columns)
    )

def read_csv_with_fallback(table_name, columns):
    """
    Read a reference/dimension table from incremental path if it exists,
    otherwise fall back to the full load path.
    """
    inc_path  = f"{HDFS_INC}/{table_name}"
    full_path = f"{HDFS_FULL}/{table_name}"
    if hdfs_exists(inc_path):
        print(f"  {table_name}: reading from incremental path")
        return read_csv(inc_path, columns)
    elif hdfs_exists(full_path):
        print(f"  {table_name}: no new rows — using full load reference data")
        return read_csv(full_path, columns)
    else:
        print(f"  ERROR: {table_name} not found in incremental or full load path")
        spark.stop()
        exit(1)


# =============================================================
#  STEP 1: LOAD ONLY INCREMENTAL RAW DATA
# =============================================================

print("\n[STEP 1] Loading ONLY incremental raw data...")
print(f"Reading from: {HDFS_INC}")

_fact_inc_path = f"{HDFS_INC}/fact_passenger_entry_exit"
if not hdfs_exists(_fact_inc_path):
    print(f"\nERROR: Incremental HDFS data not found at {_fact_inc_path}")
    print("Please run incremental Sqoop first, then re-run this script.")
    spark.stop()
    exit(1)

if not hdfs_has_data(_fact_inc_path):
    print(f"\nINFO: No new passenger records found for this incremental run.")
    print("Gold tables are already up to date — nothing to process.")
    spark.stop()
    exit(0)

fact_pax_new = read_csv(f"{HDFS_INC}/fact_passenger_entry_exit", [
    "entry_exit_id", "station_id", "date_id", "total_entry_exit",
    "estimated_entries", "estimated_exits", "record_type", "data_source", "created_at"
])

dim_date = read_csv(f"{HDFS_INC}/dim_date", [
    "date_id", "year", "quarter", "month", "is_annual",
    "period_label", "period_start", "period_end", "created_at"
])

dim_lines = read_csv_with_fallback("dim_lines", [
    "line_id", "line_name", "line_color", "is_night_service", "created_at", "updated_at"
])

dim_networks = read_csv_with_fallback("dim_networks", [
    "network_id", "network_name", "network_type", "created_at", "updated_at"
])

dim_stations = read_csv_with_fallback("dim_stations", [
    "station_id", "nlc_code", "station_name", "network_id",
    "has_london_underground", "has_elizabeth_line", "has_overground",
    "has_dlr", "has_night_tube", "is_active",
    "created_at", "updated_at"
])

fact_lines_new = read_csv_with_fallback("fact_station_lines", [
    "station_line_id", "station_id", "line_id", "is_interchange",
    "effective_from", "effective_to", "created_at"
])

print(f"  New passenger records : {fact_pax_new.count()}")
print(f"  New station-line records: {fact_lines_new.count()}")


# =============================================================
#  STEP 2: GOLD TABLE HELPERS
# =============================================================

def read_gold(table_name):
    path = f"{GOLD_BASE}/{table_name}"
    if not hdfs_has_data(path):
        print(f"  {table_name}: no existing data — will create fresh")
        return None
    df = spark.read.parquet(path)
    df.cache()
    df.count()
    return df

def merge_with_gold(existing, new_df):
    if existing is None:
        return new_df
    return existing.union(new_df)

def save_gold(df, table_name):
    path = f"{GOLD_BASE}/{table_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"  Saved -> {path}")


# =============================================================
#  STEP 3: WATERMARK CHECK
# =============================================================

print("\n[STEP 3] WATERMARK CHECK — fact_passenger_entry_exit")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HIVE_DB}.incremental_watermark (
        source_table   STRING,
        processed_year INT,
        processed_at   TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION '/tmp/yamini/tfl_project1/incremental_watermark'
""")

already_done_df = spark.sql(f"""
    SELECT processed_year
    FROM   {HIVE_DB}.incremental_watermark
    WHERE  source_table = 'fact_passenger_entry_exit'
""")
already_done_years = [r["processed_year"] for r in already_done_df.collect()]
print(f"  Years already processed : {already_done_years}")

incoming_years_df = (
    fact_pax_new
    .join(dim_date, "date_id")
    .select(col("year").cast(IntegerType()).alias("year"))
    .distinct()
)
incoming_years = [r["year"] for r in incoming_years_df.collect()]
print(f"  Incoming batch years    : {incoming_years}")

years_to_process = [y for y in incoming_years if y not in already_done_years]
print(f"  Years to load (new)     : {years_to_process}")

if not years_to_process:
    print("\n  All years already in watermark — skipping Strategy 1 tables.")

fact_pax_new_filtered = (
    fact_pax_new
    .join(dim_date, "date_id")
    .filter(col("year").cast(IntegerType()).isin(years_to_process))
) if years_to_process else None


# =============================================================
#  STEP 4: STRATEGY 1 — APPEND year-based gold tables
# =============================================================

print("\n[STEP 4] STRATEGY 1 — Append new year results")

if years_to_process:

    # gold_passengers_by_year
    print("\n--- gold_passengers_by_year ---")
    new_by_year = (
        fact_pax_new_filtered
        .groupBy("year")
        .agg(_sum("total_entry_exit").alias("total_passengers"))
        .orderBy("year")
    )
    new_by_year.show(truncate=False)
    existing = read_gold("gold_passengers_by_year")
    save_gold(merge_with_gold(existing, new_by_year).orderBy("year"), "gold_passengers_by_year")

    # gold_quarterly_trend
    print("\n--- gold_quarterly_trend ---")
    new_quarterly = (
        fact_pax_new_filtered
        .groupBy(
            col("year").cast(IntegerType()).alias("year"),
            col("quarter").cast(IntegerType()).alias("quarter")
        )
        .agg(_sum("total_entry_exit").alias("total_passengers"))
        .orderBy("year", "quarter")
    )
    new_quarterly.show(truncate=False)
    existing = read_gold("gold_quarterly_trend")
    save_gold(merge_with_gold(existing, new_quarterly).orderBy("year", "quarter"), "gold_quarterly_trend")

    # Update watermark
    print(f"\n  Updating watermark with years: {years_to_process}")
    watermark_schema = StructType([
        StructField("source_table",   StringType(),  False),
        StructField("processed_year", IntegerType(), False),
    ])
    new_watermark = (
        spark.createDataFrame(
            [("fact_passenger_entry_exit", int(y)) for y in years_to_process],
            watermark_schema
        )
        .withColumn("processed_at", current_timestamp())
    )
    new_watermark.write.mode("append").insertInto(f"{HIVE_DB}.incremental_watermark")
    print(f"  Watermark updated — years {years_to_process} marked as done.")

else:
    print("  [SKIP] No new years — Strategy 1 gold tables unchanged.")


# =============================================================
#  STEP 5: STRATEGY 2 — MERGE cross-year aggregate gold tables
# =============================================================

print("\n[STEP 5] STRATEGY 2 — Merge new results into cross-year gold tables")

# gold_busiest_stations
print("\n--- gold_busiest_stations ---")
new_station_totals = (
    fact_pax_new
    .join(dim_stations, "station_id")
    .groupBy("station_name")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
)
existing = read_gold("gold_busiest_stations")
merged   = merge_with_gold(existing, new_station_totals)
updated  = (
    merged
    .groupBy("station_name")
    .agg(_sum("total_passengers").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
)
updated.show(10, truncate=False)
save_gold(updated, "gold_busiest_stations")

# gold_passengers_by_line
print("\n--- gold_passengers_by_line ---")
new_by_line = (
    fact_pax_new
    .join(fact_lines_new, "station_id")
    .join(dim_lines, "line_id")
    .groupBy("line_name")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
)
existing = read_gold("gold_passengers_by_line")
merged   = merge_with_gold(existing, new_by_line)
updated  = (
    merged
    .groupBy("line_name")
    .agg(_sum("total_passengers").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
)
updated.show(truncate=False)
save_gold(updated, "gold_passengers_by_line")

# gold_passengers_by_network
print("\n--- gold_passengers_by_network ---")
new_by_network = (
    fact_pax_new
    .join(dim_stations, "station_id")
    .join(dim_networks, "network_id")
    .groupBy("network_name", "network_type")
    .agg(_sum("total_entry_exit").alias("total_passengers"))
)
existing = read_gold("gold_passengers_by_network")
merged   = merge_with_gold(existing, new_by_network)
updated  = (
    merged
    .groupBy("network_name", "network_type")
    .agg(_sum("total_passengers").alias("total_passengers"))
    .orderBy(desc("total_passengers"))
)
updated.show(truncate=False)
save_gold(updated, "gold_passengers_by_network")

# gold_interchange_stations
print("\n--- gold_interchange_stations ---")
new_interchange = (
    fact_lines_new
    .join(dim_stations, "station_id")
    .groupBy("station_name")
    .agg(count("line_id").alias("num_lines"))
)
existing = read_gold("gold_interchange_stations")
new_station_names = [r["station_name"] for r in new_interchange.select("station_name").collect()]
if existing is not None:
    old_others = existing.filter(~col("station_name").isin(new_station_names))
    updated_interchange = old_others.union(new_interchange).orderBy(desc("num_lines"))
else:
    updated_interchange = new_interchange.orderBy(desc("num_lines"))
updated_interchange.show(15, truncate=False)
save_gold(updated_interchange, "gold_interchange_stations")

# gold_night_tube_analysis
print("\n--- gold_night_tube_analysis ---")
new_night_tube = (
    fact_pax_new
    .join(dim_stations, "station_id")
    .groupBy(col("has_night_tube"))
    .agg(
        count("station_id").alias("num_records"),
        _sum("total_entry_exit").alias("total_passengers"),
        avg("total_entry_exit").alias("avg_passengers_per_record")
    )
)
existing = read_gold("gold_night_tube_analysis")
merged   = merge_with_gold(existing, new_night_tube)
updated  = (
    merged
    .groupBy("has_night_tube")
    .agg(
        _sum("num_records").alias("num_records"),
        _sum("total_passengers").alias("total_passengers"),
        avg("avg_passengers_per_record").alias("avg_passengers_per_record")
    )
)
updated.show(truncate=False)
save_gold(updated, "gold_night_tube_analysis")


# =============================================================
#  SUMMARY
# =============================================================

print("\n" + "=" * 60)
print("TRULY INCREMENTAL SPARK PIPELINE COMPLETE - Yamini")
print("=" * 60)
print(f"Watermark   : {already_done_years} already done, {years_to_process} newly loaded")
print("")
print("Strategy 1 - Watermark-gated append (year-based):")
if years_to_process:
    print(f"  gold_passengers_by_year  <- appended years {years_to_process}")
    print(f"  gold_quarterly_trend     <- appended quarters for {years_to_process}")
    print(f"  incremental_watermark    <- updated with years {years_to_process}")
else:
    print("  gold_passengers_by_year  <- SKIPPED (already in watermark)")
    print("  gold_quarterly_trend     <- SKIPPED (already in watermark)")
print("")
print("Strategy 2 - Merge (cross-year aggregates):")
print("  gold_busiest_stations      <- re-summed with new station totals")
print("  gold_passengers_by_line    <- re-summed with new line totals")
print("  gold_passengers_by_network <- re-summed with new network totals")
print("  gold_interchange_stations  <- refreshed from new mapping data")
print("  gold_night_tube_analysis   <- re-summed with new has_night_tube data")
print("=" * 60)

spark.stop()
