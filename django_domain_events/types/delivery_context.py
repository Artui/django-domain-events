"""What a receiver is told about the delivery it is running under."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    """Delivery metadata, passed only to receivers declaring ``takes_context``.

    Frozen data read off the event row, never a live handle: a durable delivery
    can run in another process hours after the scope that produced it has gone.
    """

    event_id: int
    event_name: str
    attempt: int
    actor_key: str
    scope: dict[str, Any]
