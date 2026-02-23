"""Kafka producer - publishes payment events to the payment.events.raw topic."""

import json
import logging
import signal
import sys
import time

from confluent_kafka import Producer, KafkaError, KafkaException

from src.generator.config import (
    EVENTS_PER_SECOND,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    LOG_LEVEL,
)
from src.generator.payment_generator import PaymentGenerator

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("kafka_producer")

_shutdown = False


def _signal_handler(signum, frame) -> None:
    global _shutdown
    logger.info("Shutdown signal received, finishing current batch...")
    _shutdown = True


def _delivery_callback(err, msg) -> None:
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.debug(
            "Delivered to %s [partition %d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def create_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "payment-generator",
        "linger.ms": 50,
        "compression.type": "snappy",
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 100,
    }
    return Producer(conf)


def run() -> None:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    generator = PaymentGenerator()
    producer = create_producer()

    interval = 1.0 / EVENTS_PER_SECOND
    events_sent = 0
    start_time = time.time()

    logger.info(
        "Starting payment event generator: %d events/sec -> topic '%s' @ %s",
        EVENTS_PER_SECOND,
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )

    try:
        while not _shutdown:
            event = generator.generate_event()
            event_dict = event.to_dict()

            # key on psp_reference so all events for one txn land on the same partition
            producer.produce(
                topic=KAFKA_TOPIC,
                key=event.psp_reference.encode("utf-8"),
                value=json.dumps(event_dict).encode("utf-8"),
                callback=_delivery_callback,
            )

            events_sent += 1

            if events_sent % 100 == 0:
                elapsed = time.time() - start_time
                rate = events_sent / elapsed if elapsed > 0 else 0
                logger.info(
                    "Events sent: %d | Rate: %.1f events/sec | Type: %s | Merchant: %s",
                    events_sent,
                    rate,
                    event.event_type,
                    event.merchant_id,
                )

            producer.poll(0)
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        logger.info("Flushing remaining messages...")
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d messages were not delivered", remaining)
        logger.info("Generator stopped. Total events sent: %d", events_sent)


if __name__ == "__main__":
    run()
