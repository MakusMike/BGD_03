
import csv
import json
import logging
import os
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "ddos-raw")

PROJECT_ROOT  = Path(__file__).resolve().parent.parent
INPUT_FILE    = PROJECT_ROOT / "data" / "raw" / "dataset.csv"
SEND_DELAY_MS = int(os.getenv("PRODUCER_DELAY_MS", "0"))

DROP_COLUMNS = {"Unnamed: 0", "Flow ID"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ddos_producer")


def build_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                batch_size=1_048_576,
                linger_ms=50,
                acks=1,
                retries=3,
            )
            logger.info("Connected to Kafka: %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not available (attempt %d/%d), retrying in %ds...",
                           attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts.")


def produce(producer: KafkaProducer) -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}\n"
            "Download from: https://www.kaggle.com/datasets/devendra416/ddos-datasets"
        )

    logger.info("Reading: %s", INPUT_FILE)
    sent = 0

    with open(INPUT_FILE, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {k: v for k, v in row.items() if k not in DROP_COLUMNS}
            producer.send(KAFKA_TOPIC, value=record)
            sent += 1
            if sent % 10_000 == 0:
                producer.flush()
                logger.info("Sent %d records...", sent)
            if SEND_DELAY_MS > 0:
                time.sleep(SEND_DELAY_MS / 1000)

    producer.flush()
    logger.info("Done. Sent: %d records to topic: %s", sent, KAFKA_TOPIC)


if __name__ == "__main__":
    producer = build_producer()
    try:
        produce(producer)
    finally:
        producer.close()
        logger.info("Producer closed.")