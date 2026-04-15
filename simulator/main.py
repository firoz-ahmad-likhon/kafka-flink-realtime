import logging
import os
import time

from simulator.publisher import KafkaEventPublisher
from simulator.service import SimulatorService

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_KAFKA_ACKS = "all"
DEFAULT_ENABLE_IDEMPOTENCE = "true"
DEFAULT_RETRIES = "2147483647"
DEFAULT_RETRY_BACKOFF_MS = "1000"
DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "5"


def main() -> None:
    """Entry."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logger = logging.getLogger(__name__)
    from confluent_kafka import Producer

    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
            "client.id": os.environ["KAFKA_CLIENT_ID"],
            "acks": os.getenv("KAFKA_ACKS", DEFAULT_KAFKA_ACKS),
            "enable.idempotence": os.getenv("KAFKA_ENABLE_IDEMPOTENCE", DEFAULT_ENABLE_IDEMPOTENCE).lower() == "true",
            "retries": int(os.getenv("KAFKA_RETRIES", DEFAULT_RETRIES)),
            "retry.backoff.ms": int(os.getenv("KAFKA_RETRY_BACKOFF_MS", DEFAULT_RETRY_BACKOFF_MS)),
            "max.in.flight.requests.per.connection": int(
                os.getenv(
                    "KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION",
                    DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                ),
            ),
        },
    )
    publisher = KafkaEventPublisher(producer=producer, logger=logger)
    simulator_service = SimulatorService(publisher=publisher)
    interval_seconds = int(os.getenv("SIMULATOR_INTERVAL_SECONDS", "60"))

    logger.info("Waiting 5 seconds for Kafka startup.")
    time.sleep(5)
    logger.info("Starting simulator for topics order=%s delivery=%s interval=%ss", "order.event", "delivery.event", interval_seconds)

    while True:
        started = time.monotonic()
        simulator_service.run_iteration()
        time.sleep(max(0.0, interval_seconds - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
