"""Running what is owed. One delivery, and a pass over all of them."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from django.db import transaction

from django_domain_events.fire import context_for
from django_domain_events.registry import registry
from django_domain_events.settings import get_codec
from django_domain_events.types.delivery_status import DeliveryStatus

if TYPE_CHECKING:
    # Annotation-only. ``from __future__ import annotations`` keeps these out
    # of the runtime import graph, which is what lets the model imports stay
    # inside the functions that query them.
    from django_domain_events.models.delivery_record import DeliveryRecord


def deliver_one(delivery: DeliveryRecord) -> DeliveryStatus:
    """Run one delivery and record its outcome, in a single transaction.

    The receiver's work and the acknowledgement commit together. For a receiver
    that touches only this database that makes delivery *effectively once*: the
    duplicate an at-least-once system owes you cannot be observed, because a
    crash before the commit rolls back the work along with the acknowledgement.
    Receivers with side effects outside the database are at-least-once, as
    promised, and that difference is worth stating in the README rather than
    burying here.
    """
    receiver = registry.receiver_for_key(delivery.receiver_key)
    if receiver is None:
        # The cost of freezing the receiver set at fire time. Terminal rather
        # than retried: no amount of waiting brings back a deleted receiver, and
        # a row retrying forever is indistinguishable from a broken one.
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
        # A payload that cannot be decoded will not decode on the next attempt
        # either, so this is terminal. One undecodable row must not stop the
        # other four thousand, and the message names the field and the vintage.
        delivery.status = DeliveryStatus.DEAD
        delivery.attempts += 1
        delivery.last_error = f"{type(exc).__name__}: {exc}"[:2000]
        delivery.completed_at = datetime.now(timezone.utc)
        delivery.save(update_fields=["status", "attempts", "last_error", "completed_at"])
        return DeliveryStatus.DEAD

    attempt = delivery.attempts + 1
    try:
        with transaction.atomic():
            if receiver.takes_context:
                receiver.func(event, context_for(delivery.event, attempt))
            else:
                receiver.func(event)
            delivery.status = DeliveryStatus.SUCCEEDED
            delivery.attempts = attempt
            delivery.completed_at = datetime.now(timezone.utc)
            delivery.save(update_fields=["status", "attempts", "completed_at"])
    except Exception as exc:
        return _fail(delivery, f"{type(exc).__name__}: {exc}", attempt=attempt)
    return DeliveryStatus.SUCCEEDED


def _fail(delivery: DeliveryRecord, message: str, attempt: int | None = None) -> DeliveryStatus:
    """Record a failed attempt, dead-lettering once the budget is spent.

    ``max_attempts`` is read off the row rather than off the live declaration,
    so lowering the limit later cannot retroactively dead-letter work already in
    flight under the old one.
    """
    delivery.attempts = attempt if attempt is not None else delivery.attempts + 1
    delivery.last_error = message[:2000]
    exhausted = delivery.attempts >= delivery.max_attempts
    delivery.status = DeliveryStatus.DEAD if exhausted else DeliveryStatus.FAILED
    delivery.completed_at = datetime.now(timezone.utc) if exhausted else None
    delivery.save(update_fields=["status", "attempts", "last_error", "completed_at"])
    return delivery.status


def deliver_pending(limit: int | None = None) -> dict[DeliveryStatus, int]:
    """Deliver everything currently owed, once, and report what happened.

    A single pass with no leasing and no ``SELECT ... FOR UPDATE SKIP LOCKED``.
    That is a real limitation and it is stated rather than implied: two of these
    running at once will both claim the same rows and deliver twice. At-least-
    once already requires receivers to tolerate a duplicate, so this is safe
    rather than merely tolerable, but it is not the concurrent relay -- that
    arrives with the leased claim, and this function's callers do not change
    when it does.
    """
    # See fire(): a module-level model import would run during app loading.
    from django_domain_events.models.delivery_record import DeliveryRecord

    query = (
        DeliveryRecord.objects.filter(status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED])
        .select_related("event")
        .order_by("available_at", "pk")
    )
    if limit is not None:
        query = query[:limit]

    counts: dict[DeliveryStatus, int] = {}
    for delivery in list(query):
        outcome = deliver_one(delivery)
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts
