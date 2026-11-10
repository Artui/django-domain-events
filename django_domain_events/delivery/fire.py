from __future__ import annotations

import asyncio
import inspect
import warnings
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from asgiref.sync import async_to_sync
from django.db import transaction
from django.utils import timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.wake import notify_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.scope.attributed import current_scope
from django_domain_events.scope.causation import (
    caused_by,
    causing_event_id,
    inherited_correlation_id,
)
from django_domain_events.scope.suppressed import suppression_for
from django_domain_events.settings import get_codec, setting
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.utils import (
    in_project_time,
    is_async_callable,
    resolve_due,
    resolve_targets,
)


def fire(
    event: object, *, dedupe_key: str | None = None, occurred_at: datetime | None = None
) -> int | None:
    """Record an event and owe it to its durable receivers."""
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

    scope = current_scope()
    suppression = suppression_for(type(event))
    if suppression is not None and not suppression[1]:
        return None

    retention_seconds, delete_when = entry.retention_columns
    if delete_when and dedupe_key is not None:
        raise ValueError(
            f"fire({entry.name}) was given a dedupe_key, but {entry.name} is deleted "
            f"once consumed ({entry.retention}), and the key protects only while the "
            f"row exists. Declare it with a timedelta retention to keep the key for a "
            f"known window, or drop the key."
        )

    dedupe_limit = EventRecord.dedupe_key.field.max_length
    assert dedupe_limit is not None
    if dedupe_key is not None and len(dedupe_key) > dedupe_limit:
        raise ValueError(
            f"fire({entry.name}) was given a dedupe_key of {len(dedupe_key)} characters; "
            f"the limit is {dedupe_limit}, the width of the column the event row stores "
            f"it in. A longer one is rejected by the database on Postgres and stored "
            f"whole on SQLite, so it would work in tests and fail in production. "
            f"Shorten the dedupe_key."
        )

    record = EventRecord.objects.create(
        name=entry.name,
        version=entry.version,
        payload=get_codec().encode(event),
        dedupe_key=dedupe_key,
        occurred_at=in_project_time(occurred_at) if occurred_at is not None else timezone.now(),
        actor_id=scope.actor.user_pk,
        actor_key=scope.actor.key,
        actor_label=scope.actor.label,
        scope=dict(scope.data),
        correlation_id=scope.correlation_id or inherited_correlation_id(),
        causation_id=causing_event_id(),
        suppressed_reason=suppression[0] if suppression is not None else "",
        retention_seconds=retention_seconds,
        delete_when=delete_when,
    )
    if suppression is not None:
        return record.pk
    context = DeliveryContext(
        event_id=record.pk,
        event_name=record.name,
        attempt=1,
        actor_key=record.actor_key,
        actor_label=record.actor_label,
        scope=record.scope,
    )

    receivers = registry.receivers_for(type(event))
    owed = [
        (r, target)
        for r in receivers
        if r.mode is DeliveryMode.DURABLE
        for target in (
            [""]
            if r.targets is None
            else resolve_targets(r.key, r.targets, event, replace(context, receiver_key=r.key))
        )
    ]
    due = {
        r.key: resolve_due(r.key, r.due, event, record.occurred_at, record.recorded_at)
        for r, _ in owed
        if r.due is not None
    }
    if owed:
        DeliveryRecord.objects.bulk_create(
            [
                DeliveryRecord(
                    event=record,
                    receiver_key=r.key,
                    target=target,
                    max_attempts=r.max_attempts,
                    available_at=due.get(r.key, record.recorded_at),
                    due_at=due.get(r.key),
                )
                for r, target in owed
            ]
        )
        eager_keys = sorted({r.key for r, _ in owed if r.eager})
        if eager_keys:
            transaction.on_commit(
                _deliver_eagerly(record.pk, eager_keys, alias), using=alias, robust=True
            )
        transaction.on_commit(notify_relay, using=alias, robust=True)

    for r in receivers:
        if r.mode is DeliveryMode.INLINE:
            with caused_by(record.pk, record.correlation_id):
                call_receiver(r.func, r.takes_context, event, replace(context, receiver_key=r.key))
        elif r.mode is DeliveryMode.ON_COMMIT:
            transaction.on_commit(
                _bind(
                    r.func,
                    r.takes_context,
                    event,
                    replace(context, receiver_key=r.key),
                    record.pk,
                    record.correlation_id,
                ),
                using=alias,
                robust=True,
            )

    return record.pk


def _deliver_eagerly(event_id: int, receiver_keys: list[str], alias: str) -> Callable[[], None]:
    """Attempt this event's deliveries in the firing process, once, after commit."""

    def run() -> None:
        from django_domain_events.delivery.claim_batch import claim_batch
        from django_domain_events.delivery.deliver import dispatch_one
        from django_domain_events.models.delivery_record import DeliveryRecord

        delivery_ids = list(
            DeliveryRecord.objects.using(alias)
            .filter(event=event_id, receiver_key__in=receiver_keys)
            .values_list("pk", flat=True)
        )
        now = timezone.now()
        claimed = claim_batch(
            worker_id="eager",
            now=now,
            lease=timedelta(
                seconds=registry.claim_lease_seconds(
                    None, default=setting("LEASE_SECONDS"), eager=True
                )
            ),
            limit=len(delivery_ids),
            only_ids=delivery_ids,
        )
        for delivery_id in claimed:
            # As the claiming worker, not the row's current owner, which may have changed since.
            dispatch_one(delivery_id, worker_id="eager")

    return run


def call_receiver(
    func: Callable[..., Any], takes_context: bool, event: object, context: DeliveryContext
) -> None:
    """Invoke a receiver with the arity its declaration promised, to completion."""
    run_to_completion(func, *((event, context) if takes_context else (event,)))


def run_to_completion(func: Callable[..., Any], *args: object) -> None:
    """Call a sync or an ``async def`` callable and return once it has finished."""
    if is_async_callable(func):
        try:
            async_to_sync(func)(*args)
        except asyncio.CancelledError as exc:
            raise RuntimeError(
                f"{_name(func)} was cancelled before it finished, so it is recorded as a failure."
            ) from exc
        return
    result = func(*args)
    if inspect.isawaitable(result) or inspect.isasyncgen(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError(
            f"{_name(func)} returned {type(result).__name__} instead of finishing its work, "
            f"so the work was not run. Declare it `async def` with no `yield` in its body, or, "
            f"if it is a sync wrapper around a coroutine function, mark it with "
            f"asgiref.sync.markcoroutinefunction."
        )


def _name(func: Callable[..., Any]) -> str:
    """The callable's qualified name for a message, or its repr where it has none."""
    return getattr(func, "__qualname__", None) or repr(func)


def _bind(
    func: Callable[..., None],
    takes_context: bool,
    event: object,
    context: DeliveryContext,
    event_id: int,
    correlation_id: UUID | None,
) -> Callable[[], None]:
    """Capture everything an ``on_commit`` callback needs, at fire time."""

    def run() -> None:
        with caused_by(event_id, correlation_id):
            call_receiver(func, takes_context, event, context)

    return run
