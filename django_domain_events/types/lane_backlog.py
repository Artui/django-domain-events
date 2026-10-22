from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from django_domain_events.types.lane_pause import LanePause


@dataclass(frozen=True, slots=True)
class LaneBacklog:
    """What one lane is behind on, and which relays hold it paused.

    The lane's receivers' backlogs added up. A row whose receiver has been
    deleted since is counted in the default lane, because that is the lane it
    drains through.
    """

    lane: str
    owed: int
    """Deliveries due and not yet settled, summed over the lane's receivers.
    See ``OutboxHealth.owed``."""

    scheduled: int
    """Deliveries whose due time has not come yet, which ``owed`` leaves out."""

    claimed: int
    """How many of ``owed`` a worker holds right now."""

    dead: int
    oldest_owed_at: datetime | None
    """When the oldest still-owed delivery in the lane became owed: the
    earliest of its receivers' ``oldest_owed_at``, or None when nothing in the
    lane is owed. This is the per-lane backlog alert's number, read the way
    ``OutboxHealth.oldest_owed_at`` is."""

    pauses: tuple[LanePause, ...]
    """Every relay holding this lane paused, by ``worker_id``; empty when none
    does. A pause is per relay, so one relay's pause says nothing about another
    relay serving the same lane."""
