from __future__ import annotations

import warnings
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from uuid import UUID

from django.db import transaction

from django_domain_events.attributed import current_scope
from django_domain_events.causation import (
    caused_by,
    causing_event_id,
    inherited_correlation_id,
)
from django_domain_events.registry import registry
from django_domain_events.settings import get_codec, setting
from django_domain_events.suppressed import suppression_for
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.wake import notify_relay
from django_domain_events.write_alias import write_alias


def fire(
    event: object, *, dedupe_key: str | None = None, occurred_at: datetime | None = None
) -> int | None:
    """Record an event and owe it to its durable receivers. Returns its id.

    Returns the event id, or ``None`` when suppression discarded it without
    recording.

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

    alias = write_alias()
    if (
        setting("WARN_OUTSIDE_ATOMIC")
        and not transaction.get_connection(using=alias).in_atomic_block
    ):
        warnings.warn(
            f"fire({entry.name}) ran outside a transaction. The event row is its "
            f"own transaction, so it can commit while the change that caused it "
            f"rolls back. Wrap the caller in transaction.atomic().",
            stacklevel=2,
        )

    # Captured here, at fire time, in the firing process. Everything downstream
    # reads attribution off the row: a durable delivery can run in another
    # process hours later, and on_commit callbacks run at commit, which can be
    # after the `with attributed(...)` block has already exited. Reading a
    # ContextVar there returns a stale answer or None, silently.
    scope = current_scope()
    suppression = suppression_for(type(event))
    if suppression is not None and not suppression[1]:
        return None

    record = EventRecord.objects.create(
        name=entry.name,
        version=entry.version,
        payload=get_codec().encode(event),
        dedupe_key=dedupe_key,
        occurred_at=occurred_at if occurred_at is not None else datetime.now(timezone.utc),
        actor_id=scope.actor.user_pk,
        actor_key=scope.actor.key,
        actor_label=scope.actor.label,
        # A copy. The stored value is handed to receivers as DeliveryContext.scope
        # and one mutation of a shared dict would reach every later event.
        scope=dict(scope.data),
        correlation_id=scope.correlation_id or inherited_correlation_id(),
        causation_id=causing_event_id(),
        suppressed_reason=suppression[0] if suppression is not None else "",
    )
    if suppression is not None:
        # Recorded, deliberately undelivered, and the reason is on the row. No
        # delivery rows, and no inline or on-commit receivers either: suppression
        # is about the event, not about one execution site.
        return record.pk
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
        rows = DeliveryRecord.objects.bulk_create(
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
        eager_ids = [row.pk for row, r in zip(rows, durable, strict=True) if r.eager]
        if eager_ids:
            transaction.on_commit(_deliver_eagerly(eager_ids), using=alias, robust=True)
        # After commit, because a notification sent before it would wake a relay
        # that cannot yet see the rows it was told about.
        transaction.on_commit(notify_relay, using=alias, robust=True)

    for r in receivers:
        if r.mode is DeliveryMode.INLINE:
            # Causation at every execution site, not only the durable one. An
            # event fired by an inline receiver is just as much a descendant.
            with caused_by(record.pk, record.correlation_id):
                call_receiver(r.func, r.takes_context, event, context)
        elif r.mode is DeliveryMode.ON_COMMIT:
            # robust=True is not optional: without it an uncaught exception in one
            # on_commit callback cancels every later one in the same transaction,
            # silently deleting the other receivers' work.
            transaction.on_commit(
                _bind(r.func, r.takes_context, event, context, record.pk, record.correlation_id),
                using=alias,
                robust=True,
            )

    return record.pk


def _deliver_eagerly(delivery_ids: list[int]) -> Callable[[], None]:
    """Attempt these deliveries in the firing process, once, after commit.

    Best effort by construction: whatever this loses to a crash is still owed,
    because the delivery row is the record and the relay reclaims it when the
    lease lapses.
    """

    def run() -> None:
        from django_domain_events.claim_batch import claim_batch
        from django_domain_events.deliver import deliver_one

        now = datetime.now(timezone.utc)
        claimed = claim_batch(
            worker_id="eager",
            now=now,
            lease=timedelta(seconds=setting("LEASE_SECONDS")),
            limit=len(delivery_ids),
            only_ids=delivery_ids,
        )
        for delivery_id in claimed:
            deliver_one(delivery_id)

    return run


def call_receiver(
    func: Callable[..., None], takes_context: bool, event: object, context: DeliveryContext
) -> None:
    """Invoke a receiver with the arity its declaration promised."""
    if takes_context:
        func(event, context)
    else:
        func(event)


def _bind(
    func: Callable[..., None],
    takes_context: bool,
    event: object,
    context: DeliveryContext,
    event_id: int,
    correlation_id: UUID | None,
) -> Callable[[], None]:
    """Capture everything an ``on_commit`` callback needs, at fire time.

    Commit can happen after the scope that set an ambient value has exited, so a
    callback reading one later gets a stale answer, silently. The cause is
    captured here for the same reason: relying on the ambient one still being
    set at commit is exactly the mistake this function's existence warns about.
    """

    def run() -> None:
        with caused_by(event_id, correlation_id):
            call_receiver(func, takes_context, event, context)

    return run
