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
    """When the oldest owed delivery became due. The age of this is the number
    an alert threshold is actually written against."""
