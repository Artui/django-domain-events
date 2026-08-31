"""The immutable log row: one per ``fire()``."""

from __future__ import annotations

from django.conf import settings
from django.db import models


class EventRecord(models.Model):
    """One fired event, recorded inside the caller's transaction.

    Named ``EventRecord`` rather than ``Event`` on purpose: the consumer's own
    declared class is the event, and this is the row that records one having been
    fired. Two things called ``Event`` in one import namespace is a tax paid on
    every future reading of the code.

    Immutable by convention. Nothing in this package updates a row after insert;
    everything mutable about a delivery lives on ``DeliveryRecord``.
    """

    name = models.CharField(max_length=255, db_index=True)
    """Registered name, ``<app_label>.<ClassName>`` by default.

    Deliberately not the dotted import path: a module move would otherwise
    orphan every pending row naming it, and a rename is a refactor people make
    without thinking about the outbox.
    """

    version = models.PositiveSmallIntegerField(default=1)
    """Payload schema version. Additive-only changes decode without it; it exists
    so that a change which is not additive has somewhere to be expressed."""

    payload = models.JSONField()
    """Codec output. Encoded with ``DjangoJSONEncoder``, decoded by whichever
    codec is configured."""

    dedupe_key = models.CharField(max_length=255, unique=True, null=True, blank=True)
    """Caller-supplied idempotency handle for producers.

    Nullable and unique: many rows may have no key, but a key names one event.
    """

    occurred_at = models.DateTimeField()
    """When the thing happened in the domain. Differs from ``recorded_at`` on a
    backfill or a replay, which is exactly when the difference matters."""

    recorded_at = models.DateTimeField(auto_now_add=True, db_index=True)
    """When the row was written. The column retention prunes on."""

    actor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="+",
    )
    """The acting user, when there was one.

    ``SET_NULL`` because a log that disappears when a user is deleted is a log
    that lies, and ``related_name="+"`` because a reverse accessor bolted onto
    the consumer's user model is a cost this package has no right to impose.
    """

    actor_key = models.CharField(max_length=255, blank=True, db_index=True)
    """Universal actor identity: ``auth.User:42``, ``system:relay``,
    ``service:billing``.

    The column to query when the answer has to cover every actor, because plenty
    of things that fire events are not users at all.
    """

    actor_label = models.CharField(max_length=255, blank=True)
    """Display snapshot, captured at fire time so it survives the actor's
    deletion."""

    scope = models.JSONField(default=dict, blank=True)
    """Ambient scope frozen at fire time. Read off the row by every downstream
    consumer, never off a ``ContextVar``."""

    correlation_id = models.UUIDField(null=True, blank=True, db_index=True)
    """The root of a causal chain, shared by every event descended from it."""

    causation_id = models.BigIntegerField(null=True, blank=True, db_index=True)
    """The event whose receiver fired this one.

    A plain integer rather than a self-referential foreign key: retention prunes
    old rows, and a cascade or a protect on the parent would make the pruner's
    job depend on the shape of a causal graph nobody is querying at that moment.
    """

    suppressed_reason = models.CharField(max_length=255, blank=True)
    """Set means: recorded deliberately, and deliberately not delivered.

    A suppressed event has no delivery rows. Writing the row anyway is the whole
    point, because a silently dropped event is unauditable.
    """

    class Meta:
        indexes = [models.Index(fields=["name", "recorded_at"])]
        verbose_name = "event record"
        verbose_name_plural = "event records"

    def __str__(self) -> str:
        return f"{self.name}#{self.pk}"
