from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

from django.db import transaction

from django_domain_events.backoff import backoff
from django_domain_events.claim_batch import claim_batch
from django_domain_events.fire import call_receiver
from django_domain_events.registry import registry
from django_domain_events.settings import get_codec, setting
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
    now = datetime.now(timezone.utc)
    row.attempts = attempt if attempt is not None else row.attempts + 1
    row.last_error = message[:2000]
    exhausted = row.attempts >= row.max_attempts
    row.status = DeliveryStatus.DEAD if exhausted else DeliveryStatus.FAILED
    row.completed_at = now if exhausted else None
    row.available_at = now + backoff(
        row.attempts,
        base=setting("BACKOFF_BASE_SECONDS"),
        cap=setting("BACKOFF_CAP_SECONDS"),
        jitter=random.random(),
    )
    row.save(update_fields=["status", "attempts", "last_error", "completed_at", "available_at"])
    return row.status


def deliver_pending(
    limit: int | None = None, *, worker_id: str = "deliver_pending"
) -> dict[DeliveryStatus, int]:
    """Claim everything currently owed, deliver it once, and report the outcome.

    Claims through the same leased path the relay uses, so a pass here and a
    running relay do not hand the same row to two receivers.
    """
    now = datetime.now(timezone.utc)
    ids = claim_batch(
        worker_id=worker_id,
        now=now,
        lease=timedelta(seconds=setting("LEASE_SECONDS")),
        limit=limit if limit is not None else setting("BATCH_SIZE"),
    )

    counts: dict[DeliveryStatus, int] = {}
    for delivery_id in ids:
        outcome = deliver_one(delivery_id)
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts
