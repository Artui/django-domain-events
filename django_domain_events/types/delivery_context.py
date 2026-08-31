"""What a receiver is told about the delivery it is running under."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    """Delivery metadata, passed only to receivers declaring ``takes_context``.

    Frozen data, never a live handle. Everything here is read off the event row
    rather than off a ``ContextVar``, because a durable delivery can run in
    another process hours after the scope that produced it has gone.

    The shape follows ``django.tasks.TaskContext``, which carries ``attempt`` for
    the same reason.
    """

    event_id: int
    """Primary key of the event row this delivery belongs to."""

    event_name: str
    """The registered name, for a receiver handling more than one event type."""

    attempt: int
    """1 on the first delivery. A receiver that must not repeat work reads this
    together with its own idempotency record, never instead of one."""

    actor_key: str
    """Universal actor identity captured at fire time, or empty for none."""

    scope: dict[str, Any]
    """The ambient scope frozen onto the event row at fire time."""
