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
    """When the oldest still-owed delivery's event was recorded. See
    ``OutboxHealth.oldest_owed_at`` for why this is not when it becomes due."""
