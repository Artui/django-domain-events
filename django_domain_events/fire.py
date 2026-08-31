from __future__ import annotations

import warnings
from collections.abc import Callable
from datetime import datetime, timezone

from django.db import transaction

from django_domain_events.registry import registry
from django_domain_events.settings import get_codec, setting
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode


def fire(
    event: object, *, dedupe_key: str | None = None, occurred_at: datetime | None = None
) -> int:
    """Record an event and owe it to its durable receivers. Returns its id.

    This does not call durable receivers; it records intent. The event row and
    one delivery row per durable receiver are written inside the caller's
    transaction, so the obligation exists if and only if the business change
    committed. A durable receiver therefore cannot signal failure back here -
    only ``INLINE`` receivers can, by raising.
    """
    # Imported here, not at module level: Django imports an app's package before
    # the app registry is ready, and this package's __init__ re-exports fire().
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    entry = registry.event_for_class(type(event))
    if entry is None:
        raise LookupError(
            f"{type(event).__name__} is not registered. Decorate it with @event, "
            f"and make sure the module declaring it is imported."
        )

    if setting("WARN_OUTSIDE_ATOMIC") and not transaction.get_connection().in_atomic_block:
        warnings.warn(
            f"fire({entry.name}) ran outside a transaction. The event row is its "
            f"own transaction, so it can commit while the change that caused it "
            f"rolls back. Wrap the caller in transaction.atomic().",
            stacklevel=2,
        )

    record = EventRecord.objects.create(
        name=entry.name,
        version=entry.version,
        payload=get_codec().encode(event),
        dedupe_key=dedupe_key,
        occurred_at=occurred_at if occurred_at is not None else datetime.now(timezone.utc),
    )
    context = DeliveryContext(
        event_id=record.pk,
        event_name=record.name,
        attempt=1,
        actor_key=record.actor_key,
        scope=record.scope,
    )

    receivers = registry.receivers_for(type(event))
    durable = [r for r in receivers if r.mode is DeliveryMode.DURABLE]
    if durable:
        DeliveryRecord.objects.bulk_create(
            [
                DeliveryRecord(
                    event=record,
                    receiver_key=r.key,
                    max_attempts=r.max_attempts,
                    available_at=record.recorded_at,
                )
                for r in durable
            ]
        )

    for r in receivers:
        if r.mode is DeliveryMode.INLINE:
            call_receiver(r.func, r.takes_context, event, context)
        elif r.mode is DeliveryMode.ON_COMMIT:
            # robust=True is not optional: without it an uncaught exception in one
            # on_commit callback cancels every later one in the same transaction,
            # silently deleting the other receivers' work.
            transaction.on_commit(_bind(r.func, r.takes_context, event, context), robust=True)

    return record.pk


def call_receiver(
    func: Callable[..., None], takes_context: bool, event: object, context: DeliveryContext
) -> None:
    """Invoke a receiver with the arity its declaration promised."""
    if takes_context:
        func(event, context)
    else:
        func(event)


def _bind(
    func: Callable[..., None], takes_context: bool, event: object, context: DeliveryContext
) -> Callable[[], None]:
    """Capture everything an ``on_commit`` callback needs, at fire time.

    Commit can happen after the scope that set an ambient value has exited, so a
    callback reading one later gets a stale answer, silently.
    """

    def run() -> None:
        call_receiver(func, takes_context, event, context)

    return run
