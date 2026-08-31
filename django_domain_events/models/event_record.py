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

    class Meta:
        indexes = [models.Index(fields=["name", "recorded_at"])]
        verbose_name = "event record"
        verbose_name_plural = "event records"

    def __str__(self) -> str:
        return f"{self.name}#{self.pk}"
