#!/usr/bin/env python3
"""
TFL Kafka Producer
Fetches live train arrival predictions from TFL API and sends to Kafka topic tfl_arrivals
"""

import requests
import json
import time
import logging

from kafka import KafkaProducer

logging.basicConfig(
    filename='/tmp/producer_success.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

KAFKA_BROKER  = 'ip-172-31-6-42.eu-west-2.compute.internal:9092'
TOPIC         = 'tfl_arrivals'
POLL_INTERVAL = 30     # seconds between API calls
RUN_DURATION  = 3600   # total seconds to run (1 hour)

# TFL API - no key needed for public data, but add yours if you have one
TFL_LINES = ['victoria', 'central', 'jubilee', 'northern', 'piccadilly']
TFL_BASE  = 'https://api.tfl.gov.uk/Line/{line}/Arrivals'


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3
    )


def fetch_arrivals(line):
    url = TFL_BASE.format(line=line)
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def main():
    producer = create_producer()
    logging.info("Producer started. Kafka broker: %s  Topic: %s", KAFKA_BROKER, TOPIC)
    print(f"Producer started → topic: {TOPIC}")

    start = time.time()
    total_sent = 0

    while time.time() - start < RUN_DURATION:
        batch_count = 0
        for line in TFL_LINES:
            try:
                arrivals = fetch_arrivals(line)
                for record in arrivals:
                    producer.send(TOPIC, value=record)
                    batch_count += 1
            except Exception as e:
                logging.error("Error fetching %s: %s", line, e)

        producer.flush()
        total_sent += batch_count
        logging.info("Sent %d messages (total: %d)", batch_count, total_sent)
        print(f"Sent {batch_count} messages  |  total: {total_sent}")

        time.sleep(POLL_INTERVAL)

    producer.close()
    logging.info("Producer finished. Total messages sent: %d", total_sent)
    print(f"Done. Total sent: {total_sent}")


if __name__ == '__main__':
    main()
