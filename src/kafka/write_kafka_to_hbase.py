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
GROUP_ID     = 'yamini_hbase_v2'
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


def _clean(v):
    return str(v).replace("'", "").strip() if v else ''

def _delay_level(time_to_station):
    try:
        t = int(time_to_station)
        if t <= 60:   return 'arriving'
        if t <= 300:  return 'close'
        if t <= 600:  return 'medium'
        return 'far'
    except Exception:
        return 'unknown'

def _peak_hours(ingested_at):
    try:
        hour = int(str(ingested_at)[11:13])
        if 7 <= hour < 10:  return 'morning_peak'
        if 16 <= hour < 20: return 'evening_peak'
        return 'off_peak'
    except Exception:
        return 'off_peak'

def build_row_key(record):
    station    = record.get('stationName', '').replace(' ', '').replace("'", "")
    line       = record.get('lineName', '').replace(' ', '').replace("'", "")
    vehicle    = record.get('vehicleId', '')
    arr_ts     = record.get('arr_timestamp', record.get('timestamp', ''))
    return f"{station}#{line}#{vehicle}#{arr_ts}"


def write_batch_to_hbase(records):
    """Write a batch of records to HBase in a single shell call."""
    commands = []
    for record in records:
        row_key     = build_row_key(record)
        station     = _clean(record.get('stationName'))
        vehicle     = _clean(record.get('vehicleId'))
        line        = _clean(record.get('lineName'))
        platform    = _clean(record.get('platformName'))
        arrival     = _clean(record.get('expectedArrival'))
        time_to_stn = _clean(record.get('timeToStation', '0'))
        location    = _clean(record.get('currentLocation'))
        direction   = _clean(record.get('direction'))
        dest        = _clean(record.get('destinationName'))
        arr_ts      = _clean(record.get('arr_timestamp', record.get('timestamp', '')))
        ingested_at = _clean(record.get('ingested_at', time.strftime('%Y-%m-%d %H:%M:%S')))

        delay_level  = _delay_level(time_to_stn)
        peak         = _peak_hours(ingested_at)

        try:
            mins = round(int(time_to_stn) / 60, 2)
        except Exception:
            mins = 0

        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:stationName',     '{station}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:vehicleId',       '{vehicle}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:lineName',        '{line}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:platformName',    '{platform}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:expectedArrival', '{arrival}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:timeToStation',   '{time_to_stn}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:timeToStationMin','{mins}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:currentLocation', '{location}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:direction',       '{direction}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:destinationName', '{dest}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:arr_timestamp',   '{arr_ts}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:ingestionTime',   '{ingested_at}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:delayLevel',      '{delay_level}'")
        commands.append(f"put '{HBASE_TABLE}', '{row_key}', 'cf:peakHours',       '{peak}'")

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
