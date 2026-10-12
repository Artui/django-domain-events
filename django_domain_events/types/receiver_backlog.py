from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class ReceiverBacklog:
    """What one receiver is behind on."""

    key: str
    owed: int
    dead: int
    oldest_owed_at: datetime | None
    """When the oldest still-owed delivery became owed. See
    ``OutboxHealth.oldest_owed_at`` for what that is read from."""

    scheduled: int = 0
    """Deliveries whose due time has not come yet, which ``owed`` leaves out.
    Defaulted and last, so adding it does not break a consumer constructing
    one - this is an exported type."""

    claimed: int = 0
    """How many of ``owed`` a worker holds right now, the per-receiver half of
    ``OutboxHealth.claimed``. A receiver whose owed rows are all claimed and
    none finishing is wedged, or its task queue is holding them, where the same
    total spread across receivers is a relay at work. Defaulted and last for the
    reason ``scheduled`` is."""
