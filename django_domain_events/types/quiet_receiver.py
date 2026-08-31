from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class QuietReceiver:
    """A declared receiver with nothing delivered to it inside the window."""

    key: str
    event_name: str
    last_succeeded_at: datetime | None
    """None means never - which is the more interesting answer of the two."""
