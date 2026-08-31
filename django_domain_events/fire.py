"""``fire()`` - record that something happened, and owe its receivers."""

from __future__ import annotations

import warnings
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from django.db import transaction

from django_domain_events.registry import registry
from django_domain_events.settings import get_codec, setting
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode

if TYPE_CHECKING:
    # Annotation-only. ``from __future__ import annotations`` keeps these out
    # of the runtime import graph, which is what lets the model imports stay
    # inside the functions that query them.
    from django_domain_events.models.event_record import EventRecord


def fire(event: object, *, dedupe_key: str | None = None, occurred_at: datetime | None = None):
    """Record an event and owe it to its durable receivers.

    This does not mean "call the receivers". It means "record intent": the event
    row and one delivery row per durable receiver are written inside the caller's
    transaction, so the obligation exists if and only if the business change
    committed. That is the whole point, and it is what closes the gap that
    ``on_commit`` leaves open, where the row is committed, the side effect never
    happened, and nothing anywhere records that it was owed.

    Because of that, a durable receiver can no longer signal failure back here.
    The raise-or-collect question applies to ``INLINE`` receivers only, which run
    before this returns and may abort the transaction by raising.
    """
    # Models are imported here rather than at module level, and this is the
    # documented ordering exception rather than a lapse. Django imports an app's
    # package before the app registry is ready, and this package's __init__
    # re-exports fire(), so a module-level model import would run during app
    # loading and raise AppRegistryNotReady. By the time anything calls fire()
    # the registry is long since populated.
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    entry = registry.event_for_class(type(event))
    if entry is None:
        raise LookupError(
            f"{type(event).__name__} is not registered. Decorate it with @event, "
            f"and make sure the module declaring it is imported (this package "
            f"autodiscovers an 'events' module in every installed app)."
        )

    if setting("WARN_OUTSIDE_ATOMIC") and not transaction.get_connection().in_atomic_block:
        warnings.warn(
            f"fire({entry.name}) ran outside a transaction. The event row is its "
            f"own transaction, so it can commit while the business change that "
            f"caused it rolls back. DURABLE is no better than ON_COMMIT here. "
            f"Wrap the caller in transaction.atomic().",
            stacklevel=2,
        )

    record = EventRecord.objects.create(
        name=entry.name,
        version=entry.version,
        payload=get_codec().encode(event),
        dedupe_key=dedupe_key,
        occurred_at=occurred_at if occurred_at is not None else datetime.now(timezone.utc),
    )

    receivers = registry.receivers_for(type(event))
    durable = [r for r in receivers if r.mode is DeliveryMode.DURABLE]
    if durable:
        # One round trip for the whole fan-out. Five durable receivers is six
        # inserts, which is the price of the guarantee and another reason this
        # is a domain-event log rather than a notification bus.
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
            # Inside the caller's transaction, and free to abort it. No delivery
            # row: its failure mode is a rollback, so nothing can be owed.
            _call(r.func, r.takes_context, event, record, attempt=1)
        elif r.mode is DeliveryMode.ON_COMMIT:
            # robust=True is not optional. Django runs on_commit callbacks in
            # order, and an uncaught exception in one registered without it means
            # no later callback in that transaction runs at all -- so one
            # best-effort receiver failing would silently delete the others.
            transaction.on_commit(
                _on_commit_call(r.func, r.takes_context, event, record), robust=True
            )

    return record


def _call(func: Any, takes_context: bool, event: object, record: EventRecord, attempt: int):
    """Invoke a receiver with the arity its declaration promised."""
    if not takes_context:
        return func(event)
    return func(event, context_for(record, attempt))


def _on_commit_call(func: Any, takes_context: bool, event: object, record: EventRecord):
    """Bind a receiver call for ``on_commit`` to run later.

    Everything the callback needs is captured here, at fire time. It must never
    read a ``ContextVar`` when it runs: commit can happen after the scope that
    set one has exited, so the callback would read a stale value or None, and it
    would do it silently.
    """

    def run() -> None:
        _call(func, takes_context, event, record, attempt=1)

    return run


def context_for(record: EventRecord, attempt: int) -> DeliveryContext:
    """Build the frozen context handed to a receiver declaring ``takes_context``."""
    return DeliveryContext(
        event_id=record.pk,
        event_name=record.name,
        attempt=attempt,
        actor_key=record.actor_key,
        scope=record.scope,
    )
