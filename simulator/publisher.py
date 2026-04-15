import json
import logging
from collections.abc import Callable
from typing import Protocol


class EventPublisher(Protocol):
    """Publisher contract used by the simulator service."""

    def publish(self, topic: str, key: str, payload: dict[str, object]) -> None:
        """Publish one event."""
        ...

    def flush(self) -> None:
        """Flush buffered events."""
        ...


class KafkaMessageMetadata(Protocol):
    """Kafka delivery metadata needed for logging."""

    def topic(self) -> str:
        """Return the Kafka topic."""
        ...

    def partition(self) -> int:
        """Return the Kafka partition."""
        ...

    def offset(self) -> int:
        """Return the Kafka offset."""
        ...


class ProducerClient(Protocol):
    """Small subset of the Kafka producer client used here."""

    def poll(self, timeout: float) -> int:
        """Serve delivery callbacks."""
        ...

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[object, KafkaMessageMetadata], None],
    ) -> None:
        """Publish one Kafka record."""
        ...

    def flush(self) -> int:
        """Flush buffered records."""
        ...


class KafkaEventPublisher:
    """Kafka-backed publisher for simulator events."""

    def __init__(self, producer: ProducerClient, logger: logging.Logger):
        """Initialize the publisher."""
        self._producer = producer
        self._logger = logger

    def _on_delivery(self, error: object, message: KafkaMessageMetadata) -> None:
        """Log Kafka delivery results."""
        if error is not None:
            self._logger.error("Kafka delivery failed: %s", error)
            return
        self._logger.info("Kafka delivery confirmed topic=%s partition=%s offset=%s", message.topic(), message.partition(), message.offset())

    def publish(self, topic: str, key: str, payload: dict[str, object]) -> None:
        """Serialize and publish one event."""
        self._producer.poll(0)  # Serve queued delivery callbacks without blocking.
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self._logger.info("Published event topic=%s key=%s", topic, key)

    def flush(self) -> None:
        """Flush buffered events."""
        self._producer.flush()
