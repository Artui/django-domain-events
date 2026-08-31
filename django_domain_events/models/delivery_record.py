from __future__ import annotations

from django.db import models

from django_domain_events.types.delivery_status import DeliveryStatus


class DeliveryRecord(models.Model):
    """What is owed to one receiver for one event.

    Separate from the event because a single outbox row cannot express
    per-receiver retries: one failing receiver must not replay or block the
    other four. Only ``DURABLE`` receivers get a row.
    """

    # Django adds event_id at runtime. The bare annotation makes it visible to
    # the type checker without entering the class dict, so field collection
    # never sees it.
    event_id: int

    event = models.ForeignKey(
        "django_domain_events.EventRecord",
        on_delete=models.CASCADE,
        related_name="deliveries",
    )
    receiver_key = models.CharField(max_length=255, db_index=True)
    status = models.CharField(
        max_length=16, choices=DeliveryStatus.choices, default=DeliveryStatus.PENDING
    )
    attempts = models.PositiveIntegerField(default=0)

    max_attempts = models.PositiveIntegerField(default=5)
    """Copied from the declaration at fire time, so lowering it later cannot
    retroactively dead-letter rows already in flight."""

    available_at = models.DateTimeField(db_index=True)
    """The backoff schedule, and what the claim query orders by.

    Never the primary key: a transaction holding a lower id can commit after one
    holding a higher id, so a row becomes visible "in the past" and a high-water
    mark skips it forever.
    """

    claimed_by = models.CharField(max_length=255, blank=True)
    claimed_at = models.DateTimeField(null=True, blank=True)
    lease_expires_at = models.DateTimeField(null=True, blank=True)

    last_error = models.TextField(blank=True)
    """Written for an operator. Never rendered to an end user."""

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
