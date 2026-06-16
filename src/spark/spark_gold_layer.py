#!/usr/bin/env python3
from pyspark.sql import SparkSession

HDFS_GOLD = '/tmp/yamini/tfl_project1/gold'


def main():
    spark = SparkSession.builder \
        .appName("TFL_Gold_Layer") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE yamini_tfl_proj")

    print("=" * 60)
    print("TFL GOLD LAYER - Writing aggregated Parquet tables")
    print("=" * 60)

    # ── 1. Busiest stations by total passenger volume ───────────────
    print("\n[1/7] gold_busiest_stations")
    spark.sql("""
        SELECT s.station_name,
               SUM(f.total_entry_exit) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_stations s ON f.station_id = s.station_id
        GROUP  BY s.station_name
        ORDER  BY total_passengers DESC
    """).write.mode("overwrite").parquet(f"{HDFS_GOLD}/gold_busiest_stations")

    # ── 2. Total passengers per year ────────────────────────────────
    print("[2/7] gold_passengers_by_year")
    spark.sql("""
        SELECT d.year,
               SUM(f.total_entry_exit) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_date d ON f.date_id = d.date_id
        GROUP  BY d.year
        ORDER  BY d.year
    """).write.mode("overwrite").parquet(f"{HDFS_GOLD}/gold_passengers_by_year")

    # ── 3. Total passengers per tube line ───────────────────────────
    print("[3/7] gold_passengers_by_line")
    spark.sql("""
        SELECT l.line_name,
               SUM(f.total_entry_exit) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   fact_station_lines sl ON f.station_id = sl.station_id
        JOIN   dim_lines l           ON sl.line_id   = l.line_id
        GROUP  BY l.line_name
        ORDER  BY total_passengers DESC
    """).write.mode("overwrite").parquet(f"{HDFS_GOLD}/gold_passengers_by_line")

    # ── 4. Total passengers per network ─────────────────────────────
    print("[4/7] gold_passengers_by_network")
    spark.sql("""
        SELECT n.network_name,
               n.network_type,
               SUM(f.total_entry_exit) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_stations s  ON f.station_id  = s.station_id
        JOIN   dim_networks n  ON s.network_id  = n.network_id
        GROUP  BY n.network_name, n.network_type
        ORDER  BY total_passengers DESC
    """).write.mode("overwrite").parquet(f"{HDFS_GOLD}/gold_passengers_by_network")

    # ── 5. Interchange stations (number of lines per station) ───────
    print("[5/7] gold_interchange_stations")
    spark.sql("""
        SELECT s.station_name,
               COUNT(sl.line_id) AS num_lines
        FROM   dim_stations s
        JOIN   fact_station_lines sl ON s.station_id = sl.station_id
        GROUP  BY s.station_name
        ORDER  BY num_lines DESC
    """).write.mode("overwrite").parquet(f"{HDFS_GOLD}/gold_interchange_stations")

    # ── 6. Quarterly ridership trend ─────────────────────────────────
    print("[6/7] gold_quarterly_trend")
    spark.sql("""
        SELECT d.year,
               d.quarter,
               SUM(f.total_entry_exit) AS total_passengers
        FROM   fact_passenger_entry_exit f
        JOIN   dim_date d ON f.date_id = d.date_id
        GROUP  BY d.year, d.quarter
        ORDER  BY d.year, d.quarter
    """).write.mode("overwrite").parquet(f"{HDFS_GOLD}/gold_night_tube_analysis")

    # ── 7. Night tube analysis (flag_1 = night tube service) ────────
    print("[7/7] gold_night_tube_analysis")
    spark.sql("""
        SELECT s.flag_1                   AS has_night_tube,
               COUNT(f.entry_exit_id)     AS num_records,
               SUM(f.total_entry_exit)    AS total_passengers,
               AVG(f.total_entry_exit)    AS avg_passengers_per_record
        FROM   fact_passenger_entry_exit f
        JOIN   dim_stations s ON f.station_id = s.station_id
        GROUP  BY s.flag_1
    """).write.mode("overwrite").parquet(f"{HDFS_GOLD}/gold_night_tube_analysis")

    print("\n" + "=" * 60)
    print("TFL GOLD LAYER COMPLETE - all 7 tables written to HDFS")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
