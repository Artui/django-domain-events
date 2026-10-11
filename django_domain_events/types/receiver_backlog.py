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
