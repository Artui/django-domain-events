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

    available_at = models.DateTimeField()
    """The backoff schedule, and what the claim query orders by.

    No plain index: the partial ones below serve the only query that reads this
    column, and a second full index on it would be written on every insert and
    read never.

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
    """When the current cycle settled. Cleared by replay and requeue, because
    a reopened row has not settled again yet."""

    succeeded_at = models.DateTimeField(null=True, blank=True)
    """When this delivery last succeeded, and never cleared.

    Separate from ``completed_at`` because the two answer different questions
    and only one of them survives a replay. "Has this receiver done any work
    lately" is the question the quiet-receiver query asks, and reading it off a
    column that replay nulls means an operator who replays yesterday's events to
    re-run a receiver they just fixed is then told that receiver has never run.
    """

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["event", "receiver_key"],
                name="unique_delivery_per_event_and_receiver",
            )
        ]
        # One index per arm of the claim query. The predicates have to match the
        # arms exactly: an index conditioned on `status = pending` cannot serve a
        # query on `status IN (pending, failed)`, so the planner falls back to a
        # full scan of the whole delivered history - which grows without bound,
        # because that is what an event log does.
        indexes = [
            models.Index(
                fields=["available_at"],
                condition=models.Q(status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED]),
                name="dde_owed_by_available_at",
            ),
            models.Index(
                fields=["lease_expires_at"],
                condition=models.Q(status=DeliveryStatus.CLAIMED),
                name="dde_claimed_by_lease",
            ),
        ]
        verbose_name = "delivery record"
        verbose_name_plural = "delivery records"

    def __str__(self) -> str:
        return f"{self.receiver_key} <- event {self.event_id} ({self.status})"
