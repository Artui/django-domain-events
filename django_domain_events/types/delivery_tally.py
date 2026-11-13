from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DeliveryTally:
    """One event's deliveries counted by outcome; every delivery is in exactly one field."""

    owed: int
    """Pending, failed or claimed: not settled yet, due or scheduled."""

    succeeded: int
    cancelled: int
    """Cancelled by ``cancel_deliveries`` or settled by ``Moot``, which store the same status."""

    dead: int
    orphaned: int
    total: int
    """Every delivery the event has, the sum of the five fields above."""
