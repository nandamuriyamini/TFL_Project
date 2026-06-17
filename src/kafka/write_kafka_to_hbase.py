#!/usr/bin/env python3
"""
TFL Kafka → HBase Consumer
Reads TFL arrival predictions from Kafka topic tfl_arrivals
and writes them to HBase table tfl_arrivals
"""

import json
import logging
import subprocess
import time

from kafka import KafkaConsumer

logging.basicConfig(
    filename='/tmp/hbase_consumer.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

KAFKA_BROKER = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
TOPIC        = 'tfl_arrivals'
HBASE_TABLE  = 'yamini_tfl_arrivals'
GROUP_ID     = 'yamini_tfl_hbase_consumer'
BATCH_SIZE   = 20    # write to HBase every N messages


def ensure_table_exists():
    """Create HBase table if it does not already exist."""
    check_cmd = f"exists '{HBASE_TABLE}'\nexit\n"
    result = subprocess.run(
        ['hbase', 'shell', '-n'],
        input=check_cmd.encode('utf-8'),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30
    )
    output = result.stdout.decode('utf-8', errors='ignore')
    if 'does not exist' not in output:
        logging.info("HBase table %s already exists", HBASE_TABLE)
        return
    create_cmd = f"create '{HBASE_TABLE}', 'cf'\nexit\n"
    subprocess.run(
        ['hbase', 'shell', '-n'],
        input=create_cmd.encode('utf-8'),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30
    )
    logging.info("Created HBase table %s", HBASE_TABLE)
    print(f"Created HBase table: {HBASE_TABLE}")


def build_row_key(record):
    station   = record.get('stationName', '').replace(' ', '')
    vehicle   = record.get('vehicleId', '')
    timestamp = record.get('timestamp', '')
    return f"{station}_{vehicle}_{timestamp}"


def write_batch_to_hbase(records):
    """Write a batch of records to HBase in a single shell call."""
    commands = []
    for record in records:
        row_key  = build_row_key(record)
        station  = record.get('stationName', '').replace("'", "")
        vehicle  = record.get('vehicleId', '').replace("'", "")
        line     = record.get('lineName', '').replace("'", "")
        platform = record.get('platformName', '').replace("'", "")
        arrival  = record.get('expectedArrival', '').replace("'", "")

        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:station', '{station}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:vehicle', '{vehicle}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:line', '{line}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:platform', '{platform}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:arrival', '{arrival}'")

    hbase_input = '\n'.join(commands) + '\nexit\n'

    result = subprocess.run(
        ['hbase', 'shell', '-n'],
        input=hbase_input.encode('utf-8'),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60
    )
    stderr_text = result.stderr.decode('utf-8', errors='ignore').strip()
    if result.returncode != 0 and stderr_text:
        logging.error("HBase write error: %s", stderr_text)
    else:
        logging.info("Wrote %d records to HBase", len(records))


def main():
    ensure_table_exists()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True
    )

    logging.info("Consumer started. Topic: %s  HBase table: %s", TOPIC, HBASE_TABLE)
    print(f"Consumer started → reading from: {TOPIC}  writing to HBase: {HBASE_TABLE}")

    batch  = []
    total  = 0

    for message in consumer:
        try:
            batch.append(message.value)

            if len(batch) >= BATCH_SIZE:
                write_batch_to_hbase(batch)
                total += len(batch)
                print(f"Written {total} records to HBase")
                batch = []

        except Exception as e:
            logging.error("Error processing message: %s", e)

    # flush remaining
    if batch:
        write_batch_to_hbase(batch)
        total += len(batch)

    consumer.close()
    logging.info("Consumer finished. Total records written: %d", total)
    print(f"Done. Total written: {total}")


if __name__ == '__main__':
    main()
