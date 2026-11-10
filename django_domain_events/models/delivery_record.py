from __future__ import annotations

from django.db import models

from django_domain_events.models.target_digest_field import TargetDigestField
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.utils import RETRYABLE


class DeliveryRecord(models.Model):
    """What is owed to one receiver for one event - or to one of its targets."""

    event = models.ForeignKey(
        "django_domain_events.EventRecord",
        on_delete=models.CASCADE,
        related_name="deliveries",
        db_index=False,
    )
    receiver_key = models.CharField(max_length=255)
    """Indexed only as the leading column of ``dde_last_success``.

    Every lookup *by* key is an equality - the admin's receiver filter,
    ``requeue_dead``, the quiet-receiver probe - and a composite serves those
    through its leading column. The queries that group by key find their rows
    through a status index first. The single-column index it replaced also
    brought a ``varchar_pattern_ops`` copy on Postgres for ``LIKE 'prefix%'``,
    which nothing here issues: the admin searches with ``icontains``, which no
    btree can serve.
    """

    target = models.TextField(blank=True, default="")
    """Which of a fan-out receiver's targets this delivery is for, or blank.

    Blank for every receiver declared without ``targets=``, which is every
    receiver there was before the column existed - so the migration's default
    is what those rows would have been written with anyway, and nothing needs
    backfilling. A fan-out receiver never writes a blank one: ``fire()``
    refuses an empty target rather than let it read as "not a fan-out".

    Text rather than a bounded string: a target is whatever a consumer's
    callable names, and this package imposes no length of its own on it.

    In no index, and that is what makes the text safe to leave unbounded: a
    btree entry has a size limit on Postgres, and some backends refuse a text
    column in a unique index outright. Uniqueness is enforced on
    ``target_digest`` instead, and lookups go through it.
    """

    target_digest = TargetDigestField()
    """SHA-256 of ``target`` as 32 raw bytes, derived on every write, and what
    the unique constraint covers. The blank target has a digest like any other,
    so a row from a receiver without ``targets=`` is not a special case."""
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

    due_at = models.DateTimeField(null=True, blank=True)
    """When this row is or becomes owed, and never rewritten by the backoff.

    The other half of ``available_at``: that column moves on every failed
    attempt, so it cannot say how long a delivery has been owed. This one is
    set when a row becomes owed - by being written, or reopened by a replay or
    a requeue - and a bound measured in time rather than attempts is measured
    from it.

    NULL is read as the event's ``recorded_at``, which is when a row nobody has
    reopened became owed. That reading is what lets the column arrive on a
    populated table with nothing to backfill, and lets a row written before it
    existed mean exactly what it did.

    No index: nothing filters or orders on it yet.
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
            # The target is part of the identity: one receiver may owe one event to many targets.
            models.UniqueConstraint(
                fields=["event", "receiver_key", "target_digest"],
                name="unique_delivery_per_event_receiver_and_target",
            )
        ]
        indexes = [
            models.Index(
                fields=["available_at"],
                condition=models.Q(status__in=list(RETRYABLE)),
                name="dde_owed_by_available_at",
            ),
            models.Index(
                fields=["lease_expires_at"],
                condition=models.Q(status=DeliveryStatus.CLAIMED),
                name="dde_claimed_by_lease",
            ),
            models.Index(
                fields=["receiver_key"],
                condition=models.Q(status=DeliveryStatus.DEAD),
                name="dde_dead_by_receiver",
            ),
            models.Index(
                fields=["receiver_key", "succeeded_at"],
                name="dde_last_success",
            ),
            models.Index(
                fields=["event"],
                condition=~models.Q(status=DeliveryStatus.SUCCEEDED),
                name="dde_unfinished_by_event",
            ),
        ]
        verbose_name = "delivery record"
        verbose_name_plural = "delivery records"

    def __str__(self) -> str:
        # Traverses the event so the row reads with its name, not only the id.
        return f"{self.receiver_key} <- {self.event} ({self.status})"
