"""One row per (event, durable receiver): the debt, and its state."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.db import models

from django_domain_events.types.delivery_status import DeliveryStatus


class DeliveryRecord(models.Model):
    """What is owed to one receiver for one event.

    Separate from the event because a single outbox row per event cannot express
    per-receiver retries, which is the first thing anyone needs: one failing
    receiver must not replay or block the other four.

    Only ``DURABLE`` receivers get a row. ``INLINE`` and ``ON_COMMIT`` receivers
    are recorded on the event or not at all, which is the honest expression of
    what they promise.
    """

    if TYPE_CHECKING:
        # Django adds ``event_id`` at runtime from the ForeignKey below. It is
        # an annotation only, so Django's field collection never sees it.
        event_id: int

    event = models.ForeignKey(
        "django_domain_events.EventRecord",
        on_delete=models.CASCADE,
        related_name="deliveries",
    )
    """Cascade, so pruning an event takes its deliveries with it and retention
    stays a single delete rather than an ordering problem."""

    receiver_key = models.CharField(max_length=255, db_index=True)
    """Stable key, not a dotted path.

    The receiver set freezes at fire time, so this string has to survive a
    function being renamed or moved. When it does not, the row becomes
    ``ORPHANED`` and a system check says so.
    """

    status = models.CharField(
        max_length=16,
        choices=DeliveryStatus.choices,
        default=DeliveryStatus.PENDING,
    )

    attempts = models.PositiveIntegerField(default=0)
    """Incremented before the receiver runs, so a receiver that dies mid-call
    still counts as having been attempted."""

    max_attempts = models.PositiveIntegerField(default=5)
    """Copied from the receiver declaration at fire time rather than read live,
    so changing the declaration does not silently re-open rows that already
    dead-lettered under the old limit."""

    available_at = models.DateTimeField(db_index=True)
    """When this becomes claimable. The backoff schedule lives here.

    The claim query orders by this column and never by the primary key. Ordering
    outbox work by an auto-increment id is the classic way to lose an event: a
    transaction holding a lower id can commit after one holding a higher id, so
    a row becomes visible "in the past" and a high-water mark skips it forever.
    """

    claimed_by = models.CharField(max_length=255, blank=True)
    """Worker identity holding the lease. Blank when unclaimed."""

    claimed_at = models.DateTimeField(null=True, blank=True)
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    """A claim is leased, not marked-and-hoped: a worker that dies without
    acknowledging becomes re-claimable when the lease lapses, and that is the
    same path as an ordinary retry rather than a special case."""

    last_error = models.TextField(blank=True)
    """Truncated exception text, written for an operator.

    Never render this to an end user: exception messages are written by and for
    people with database access.
    """

    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["event", "receiver_key"],
                name="unique_delivery_per_event_and_receiver",
            )
        ]
        indexes = [
            models.Index(
                fields=["available_at"],
                condition=models.Q(status=DeliveryStatus.PENDING),
                name="dde_pending_by_available_at",
            )
        ]
        verbose_name = "delivery record"
        verbose_name_plural = "delivery records"

    def __str__(self) -> str:
        return f"{self.receiver_key} <- event {self.event_id} ({self.status})"
