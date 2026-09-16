from __future__ import annotations

from django.db import models

from django_domain_events.types.delivery_status import DeliveryStatus


class DeliveryRecord(models.Model):
    """What is owed to one receiver for one event - or to one of its targets.

    Separate from the event because a single outbox row cannot express
    per-receiver retries: one failing receiver must not replay or block the
    other four. Only ``DURABLE`` receivers get a row, and a receiver declared
    with ``targets=`` gets one per target, for the same reason one step down:
    one failing target must not drag the others through its retries.
    """

    event = models.ForeignKey(
        "django_domain_events.EventRecord",
        on_delete=models.CASCADE,
        related_name="deliveries",
    )
    receiver_key = models.CharField(max_length=255, db_index=True)

    target = models.TextField(blank=True, default="")
    """Which of a fan-out receiver's targets this delivery is for, or blank.

    Blank for every receiver declared without ``targets=``, which is every
    receiver there was before the column existed - so the migration's default
    is what those rows would have been written with anyway, and nothing needs
    backfilling. A fan-out receiver never writes a blank one: ``fire()``
    refuses an empty target rather than let it read as "not a fan-out".

    Text rather than a bounded string: a target is whatever a consumer's
    callable names, and this package imposes no length of its own on it.

    No index of its own. The unique constraint below leads with the event, and
    every query that reads this column also names the event.
    """
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
            # The target is part of the identity, which is what lets one
            # receiver owe one event to many targets. A receiver without
            # targets= writes the blank target, so for it this is exactly the
            # (event, receiver_key) constraint it replaced.
            models.UniqueConstraint(
                fields=["event", "receiver_key", "target"],
                name="unique_delivery_per_event_receiver_and_target",
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
        # Traverses the relation rather than reading ``event_id``, and gains
        # rather than costs: ``EventRecord.__str__`` is ``name#pk``, so this
        # reads "receiver <- shop.OrderPlaced#42" where the id alone read
        # "receiver <- event 42". An operator gets the join key *and* what the
        # event was.
        #
        # The traversal costs one query on an instance that did not fetch the
        # event, so it is worth knowing where this actually renders: **not** the
        # changelist, whose first ``list_display`` column is ``receiver_key``,
        # so ``__str__`` is never its link text. It renders on the delete
        # confirmation page, in object history, and in related-field widgets.
        # Bounded, and the same trade every other model in the family makes.
        #
        # A growth test over the changelist was written to guard this and then
        # deleted: it passed with ``list_select_related`` removed, because
        # ``list_display`` names ``event`` and Django select_relates on its own
        # whenever a related field appears there. A test that cannot fail is
        # worse than no test, and this one was also aimed at the wrong page.
        #
        # It also removes the ``event_id: int`` annotation this file used to
        # carry for ty's benefit. That was the smaller reason and it is a
        # welcome side effect, not the argument.
        return f"{self.receiver_key} <- {self.event} ({self.status})"
