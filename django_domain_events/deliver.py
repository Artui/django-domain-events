"""Running what is owed."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from django.db import transaction

from django_domain_events.fire import call_receiver
from django_domain_events.registry import registry
from django_domain_events.settings import get_codec
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_status import DeliveryStatus


def deliver_one(delivery_id: int) -> DeliveryStatus:
    """Run one delivery and record its outcome, in a single transaction.

    The receiver's work and the acknowledgement commit together, so a receiver
    touching only this database is effectively once: the duplicate at-least-once
    owes you cannot be observed. Side effects outside the database are
    at-least-once, as promised.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    delivery = DeliveryRecord.objects.select_related("event").get(pk=delivery_id)

    receiver = registry.receiver_for_key(delivery.receiver_key)
    if receiver is None:
        # Terminal: no amount of waiting brings back a deleted receiver, and a
        # row retrying forever is indistinguishable from a broken one.
        delivery.status = DeliveryStatus.ORPHANED
        delivery.last_error = (
            f"No receiver is registered under {delivery.receiver_key!r}. It was "
            f"renamed, moved or deleted after this delivery was recorded."
        )
        delivery.completed_at = datetime.now(timezone.utc)
        delivery.save(update_fields=["status", "last_error", "completed_at"])
        return DeliveryStatus.ORPHANED

    event_entry = registry.event_for_name(delivery.event.name)
    if event_entry is None:
        return _fail(
            delivery,
            f"No event is registered under {delivery.event.name!r}, so its "
            f"payload cannot be rebuilt.",
        )

    try:
        event = get_codec().decode(
            event_entry.event_class, delivery.event.payload, delivery.event.version
        )
    except Exception as exc:
        # Terminal: it will not decode on the next attempt either, and one
        # undecodable row must not stop the other four thousand.
        delivery.status = DeliveryStatus.DEAD
        delivery.attempts += 1
        delivery.last_error = f"{type(exc).__name__}: {exc}"[:2000]
        delivery.completed_at = datetime.now(timezone.utc)
        delivery.save(update_fields=["status", "attempts", "last_error", "completed_at"])
        return DeliveryStatus.DEAD

    attempt = delivery.attempts + 1
    context = DeliveryContext(
        event_id=delivery.event.pk,
        event_name=delivery.event.name,
        attempt=attempt,
        actor_key=delivery.event.actor_key,
        scope=delivery.event.scope,
    )
    try:
        with transaction.atomic():
            call_receiver(receiver.func, receiver.takes_context, event, context)
            delivery.status = DeliveryStatus.SUCCEEDED
            delivery.attempts = attempt
            delivery.completed_at = datetime.now(timezone.utc)
            delivery.save(update_fields=["status", "attempts", "completed_at"])
    except Exception as exc:
        return _fail(delivery, f"{type(exc).__name__}: {exc}", attempt=attempt)
    return DeliveryStatus.SUCCEEDED


def _fail(row: Any, message: str, attempt: int | None = None) -> DeliveryStatus:
    """Record a failed attempt, dead-lettering once the budget is spent."""
    row.attempts = attempt if attempt is not None else row.attempts + 1
    row.last_error = message[:2000]
    exhausted = row.attempts >= row.max_attempts
    row.status = DeliveryStatus.DEAD if exhausted else DeliveryStatus.FAILED
    row.completed_at = datetime.now(timezone.utc) if exhausted else None
    row.save(update_fields=["status", "attempts", "last_error", "completed_at"])
    return row.status


def deliver_pending(limit: int | None = None) -> dict[DeliveryStatus, int]:
    """Deliver everything currently owed, once, and report what happened.

    A single pass, with no leased claim and no ``SELECT ... FOR UPDATE SKIP
    LOCKED``: two of these running at once will both claim the same rows and
    deliver twice. At-least-once already requires receivers to tolerate that, but
    this is not the concurrent relay.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    query = DeliveryRecord.objects.filter(
        status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED]
    ).order_by("available_at", "pk")
    if limit is not None:
        query = query[:limit]

    counts: dict[DeliveryStatus, int] = {}
    for delivery_id in list(query.values_list("pk", flat=True)):
        outcome = deliver_one(delivery_id)
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts
