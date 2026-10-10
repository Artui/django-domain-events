from __future__ import annotations

from django.conf import settings
from django.db import models

from django_domain_events.models.delivery_record import DeliveryRecord


class EventRecord(models.Model):
    """One fired event, recorded inside the caller's transaction.

    ``EventRecord`` rather than ``Event`` because the consumer's declared class is
    the event; this is the row recording that one was fired.
    """

    # Django adds the reverse accessor at runtime. The bare annotation makes it
    # visible to the type checker without entering the class dict, the same
    # trick DeliveryRecord uses for event_id. Importing DeliveryRecord here is
    # safe in one direction only: its FK names this model by string, so it does
    # not import back.
    deliveries: models.Manager[DeliveryRecord]

    name = models.CharField(max_length=255, db_index=True)
    version = models.PositiveSmallIntegerField(default=1)
    payload = models.JSONField()
    dedupe_key = models.CharField(max_length=255, unique=True, null=True, blank=True)

    occurred_at = models.DateTimeField()
    """Domain time. Differs from ``recorded_at`` on a backfill or a replay."""

    recorded_at = models.DateTimeField(auto_now_add=True, db_index=True)

    # SET_NULL with a label snapshot: a log that loses its actor when the user is
    # deleted is a log that lies. related_name="+" keeps the reverse accessor off
    # the consumer's user model.
    actor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )
    actor_key = models.CharField(max_length=255, blank=True, db_index=True)
    """Universal identity: ``auth.User:42``, ``system:relay``. Plenty of things
    that fire events are not users."""

    actor_label = models.CharField(max_length=255, blank=True)
    scope = models.JSONField(default=dict, blank=True)
    correlation_id = models.UUIDField(null=True, blank=True, db_index=True)

    # A plain integer, not a self-reference: retention prunes old rows, and a
    # cascade would make the pruner depend on the shape of a causal graph.
    causation_id = models.BigIntegerField(null=True, blank=True, db_index=True)

    suppressed_reason = models.CharField(max_length=255, blank=True)
    """Set means recorded deliberately, and deliberately not delivered.

    Written by ``fire()`` when a ``suppressed()`` block covers the event. A
    suppressed row has no delivery rows: recording it with its reason is the
    point, because a silently dropped event is unauditable.
    """

    retention_seconds = models.PositiveIntegerField(null=True, blank=True)
    """How long this event is kept, when its declaration says; NULL follows
    ``RETENTION_DAYS``.

    Copied from the declaration at fire time, as ``max_attempts`` is copied
    onto a delivery, so re-declaring or deleting the event class cannot change
    the fate of events already recorded. NULL is also what every event recorded
    before the column existed reads as, which is the window they were recorded
    under.
    """

    delete_when = models.CharField(max_length=16, blank=True, default="")
    """Delete the event as soon as its deliveries reach this state, rather than
    when a retention window passes. Copied at fire time, like
    ``retention_seconds``.

    Blank for the ordinary window. ``"succeeded"`` waits for every delivery to
    succeed, so a dead letter keeps its event inspectable and requeueable;
    ``"settled"`` waits only for every delivery to be terminal. The declaration
    offers one retention knob, so an event carries at most one of this and
    ``retention_seconds``; nothing in the schema enforces it, because the
    check-constraint keyword changed inside the supported Django range and one
    migration cannot spell it for both ends.

    No ``choices``: the values belong to the declaration that writes them, and
    a choice list here would make every change to that a migration.
    """

    class Meta:
        indexes = [
            models.Index(fields=["name", "recorded_at"]),
            # What the prune sweep reads to find events with a policy of
            # their own: one partial index per kind, so the ordinary event -
            # nearly every row - costs neither anything, and each stays as
            # small as the set of such events still alive. The ordinary window
            # keeps using the ``recorded_at`` index.
            #
            # A query uses a partial index only if its WHERE clause implies
            # the index's condition. One index over both kinds, conditioned on
            # their OR, served both sweeps only by having each read every
            # entry of the other kind too; split, each sweep reads only its
            # own.
            #
            # A window of its own is per row, so it filters this index rather
            # than bounding a range of it: the sweep reads every live event
            # with one.
            models.Index(
                fields=["recorded_at"],
                condition=models.Q(retention_seconds__isnull=False),
                name="dde_own_window",
            ),
            # Policy first, so ``delete_when = 'succeeded'`` is one range and
            # each policy's sweep reads only its own events, oldest first.
            models.Index(
                fields=["delete_when", "recorded_at"],
                condition=~models.Q(delete_when=""),
                name="dde_consumed_by_policy",
            ),
        ]
        verbose_name = "event record"
        verbose_name_plural = "event records"

    def __str__(self) -> str:
        return f"{self.name}#{self.pk}"
