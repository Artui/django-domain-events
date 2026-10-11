from __future__ import annotations

from django.db import models


class DeliveryStatus(models.TextChoices):
    """Status of one delivery: an event, a durable receiver, and its target if any."""

    PENDING = "pending", "Pending"
    CLAIMED = "claimed", "Claimed"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    """Raised, with attempts remaining. Distinct from PENDING so that "has this
    ever failed" is answerable without reading the attempt count."""

    DEAD = "dead", "Dead"
    ORPHANED = "orphaned", "Orphaned"
    """Addressed to a receiver key the registry no longer has."""

    CANCELLED = "cancelled", "Cancelled"
    """Settled by ``cancel_deliveries`` before it ran: the work was known to be
    moot ahead of time, so it never will. Not a success, because the receiver
    did not run, and not a dead letter, because nothing failed."""
