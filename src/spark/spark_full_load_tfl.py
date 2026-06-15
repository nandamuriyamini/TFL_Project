#!/usr/bin/env python3
"""
TFL Full Load - Spark Batch Job
Reads ALL historical TFL arrival data from Kafka topic tfl_arrivals,
deduplicates, partitions by line and date, and writes Parquet to HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_date, year, month,
    count as spark_count
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BROKER = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
KAFKA_TOPIC  = 'tfl_arrivals'
HDFS_OUTPUT  = '/tmp/yamini/tfl_full_load/output'

SCHEMA = StructType([
    StructField("id",              StringType(),  True),
    StructField("vehicleId",       StringType(),  True),
    StructField("stationName",     StringType(),  True),
    StructField("lineName",        StringType(),  True),
    StructField("platformName",    StringType(),  True),
    StructField("expectedArrival", StringType(),  True),
    StructField("timeToStation",   IntegerType(), True),
    StructField("currentLocation", StringType(),  True),
    StructField("direction",       StringType(),  True),
    StructField("destinationName", StringType(),  True),
    StructField("timestamp",       StringType(),  True),
])


def main():
    spark = SparkSession.builder \
        .appName("TFL_Full_Load") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("TFL FULL LOAD - Reading all Kafka messages")
    print(f"Topic  : {KAFKA_TOPIC}")
    print(f"Output : {HDFS_OUTPUT}")
    print("=" * 60)

    # ── 1. Read ALL messages from Kafka (batch, not streaming) ──────────────
    raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    total_raw = raw.count()
    print(f"\nTotal raw Kafka messages : {total_raw:,}")

    # ── 2. Parse JSON payload ───────────────────────────────────────────────
    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("d"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_ingest_time")
    ).select(
        "d.*",
        "kafka_offset",
        "kafka_ingest_time"
    ).withColumn("loaded_at", current_timestamp()) \
     .withColumn("arrival_date", to_date(col("expectedArrival")))

    # ── 3. Drop records where JSON parsing failed (id + vehicleId both null) ─
    valid = parsed.filter(
        col("id").isNotNull() & col("vehicleId").isNotNull()
    )

    # ── 4. Deduplicate on id (each prediction ID is unique per vehicle+stop) ─
    deduped = valid.dropDuplicates(["id"])

    total_valid  = valid.count()
    total_deduped = deduped.count()
    print(f"Valid records (JSON parsed)  : {total_valid:,}")
    print(f"After deduplication (by id) : {total_deduped:,}")
    print(f"Duplicates removed          : {total_valid - total_deduped:,}")

    # ── 5. Write partitioned Parquet to HDFS ────────────────────────────────
    deduped.write \
        .mode("overwrite") \
        .partitionBy("lineName", "arrival_date") \
        .parquet(HDFS_OUTPUT)

    print(f"\nFull load written to HDFS : {HDFS_OUTPUT}")

    # ── 6. Summary by tube line ─────────────────────────────────────────────
    print("\n--- Record count per tube line ---")
    deduped.groupBy("lineName") \
        .agg(spark_count("*").alias("record_count")) \
        .orderBy("record_count", ascending=False) \
        .show(30, truncate=False)

    # ── 7. Summary by date ──────────────────────────────────────────────────
    print("--- Record count per arrival date ---")
    deduped.groupBy("arrival_date") \
        .agg(spark_count("*").alias("record_count")) \
        .orderBy("arrival_date") \
        .show(30, truncate=False)

    print("=" * 60)
    print("TFL FULL LOAD COMPLETE")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
