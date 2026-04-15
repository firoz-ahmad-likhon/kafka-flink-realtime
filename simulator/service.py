import random
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypedDict
from uuid import uuid4

from simulator.publisher import EventPublisher

ORDER_TOPIC = "order.event"
DELIVERY_TOPIC = "delivery.event"


class PendingOrder(TypedDict):
    """Pending order awaiting delivery publication."""

    order_id: str
    created_at: datetime


@dataclass(slots=True)
class SimulatorMessage:
    """Message prepared for Kafka publication."""

    topic: str
    key: str
    payload: dict[str, object]


class SimulatorService:
    """Generate and publish simulator events."""

    def __init__(self, publisher: EventPublisher):
        """Initialize simulator state."""
        self._publisher = publisher
        self._pending_orders: deque[PendingOrder] = deque()
        self._random = random.Random()

    def run_iteration(self) -> None:
        """Publish one order event and optionally one delivery event."""
        for message in self._build_messages():
            self._publisher.publish(
                topic=message.topic,
                key=message.key,
                payload=message.payload,
            )
        self._publisher.flush()

    def _build_messages(self) -> list[SimulatorMessage]:
        """Build the next set of events."""
        created_at = datetime.now(tz=UTC)
        order: PendingOrder = {
            "order_id": f"order-{uuid4().hex[:12]}",
            "created_at": created_at,
        }
        messages: list[SimulatorMessage] = [
            SimulatorMessage(
                topic=ORDER_TOPIC,
                key=order["order_id"],
                payload={
                    "order_id": order["order_id"],
                    "created_at": created_at.isoformat().replace("+00:00", "Z"),
                },
            ),
        ]
        if self._pending_orders:
            delivered_order = self._pending_orders.popleft()
            delivered_at = datetime.now(tz=UTC)
            messages.append(
                SimulatorMessage(
                    topic=DELIVERY_TOPIC,
                    key=delivered_order["order_id"],
                    payload={
                        "order_id": delivered_order["order_id"],
                        "delivered_at": delivered_at.isoformat().replace("+00:00", "Z"),
                    },
                ),
            )
        self._pending_orders.append(order)
        return messages
