from __future__ import annotations

from django.db import models


class DeliveryStatus(models.TextChoices):
    """Status of one (event, durable receiver) pair."""

    PENDING = "pending", "Pending"
    CLAIMED = "claimed", "Claimed"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    """Raised, with attempts remaining. Distinct from PENDING so that "has this
    ever failed" is answerable without reading the attempt count."""

    DEAD = "dead", "Dead"
    ORPHANED = "orphaned", "Orphaned"
    """Addressed to a receiver key the registry no longer has."""
