"""The lifecycle of a single durable delivery."""

from __future__ import annotations

from django.db import models


class DeliveryStatus(models.TextChoices):
    """Status of one (event, durable receiver) pair.

    A delivery is claimed rather than marked, so every terminal state is reached
    by a writer that held the claim. The claim fields arrive with the relay; at
    this version the statuses are already whole so that no consumer's rows need a
    data migration when they do.
    """

    PENDING = "pending", "Pending"
    """Owed, and available to be claimed once ``available_at`` has passed."""

    CLAIMED = "claimed", "Claimed"
    """Leased by a worker. A lapsed lease returns the row to ``PENDING``."""

    SUCCEEDED = "succeeded", "Succeeded"
    """The receiver returned. For a receiver touching only this database, the
    acknowledgement committed in the same transaction as its work."""

    FAILED = "failed", "Failed"
    """The receiver raised and attempts remain. Distinct from ``PENDING`` so that
    "has this ever failed" is answerable without reading the attempt count."""

    DEAD = "dead", "Dead"
    """Attempts exhausted. Requeued only by an operator."""

    ORPHANED = "orphaned", "Orphaned"
    """Addressed to a receiver key the registry no longer has.

    The cost of freezing the receiver set at fire time: a receiver can be deleted
    while rows addressed to it are still pending. Terminal, and surfaced by a
    system check rather than discovered in a log.
    """
