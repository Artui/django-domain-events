from __future__ import annotations

from django.conf import settings
from django.db import models

from django_domain_events.models.delivery_record import DeliveryRecord


class EventRecord(models.Model):
    """One fired event, recorded inside the caller's transaction."""

    deliveries: models.Manager[DeliveryRecord]

    name = models.CharField(max_length=255, db_index=True)
    version = models.PositiveSmallIntegerField(default=1)
    payload = models.JSONField()
    dedupe_key = models.CharField(max_length=255, unique=True, null=True, blank=True)

    occurred_at = models.DateTimeField()
    """Domain time. Differs from ``recorded_at`` on a backfill or a replay."""

    recorded_at = models.DateTimeField(auto_now_add=True, db_index=True)

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
            # One partial index per policy: the ordinary event, nearly every row, costs neither.
            models.Index(
                fields=["recorded_at"],
                condition=models.Q(retention_seconds__isnull=False),
                name="dde_own_window",
            ),
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
