from __future__ import annotations

import asyncio
import inspect
import warnings
from collections.abc import Callable
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
    """Record an event and owe it to its durable receivers. Returns its id.

    Returns the event id, or ``None`` when suppression discarded it without
    recording.

    This does not call durable receivers; it records intent. The event row and
    one delivery row per durable receiver are written inside the caller's
    transaction, so the obligation exists if and only if the business change
    committed. A durable receiver therefore cannot signal failure back here -
    only ``INLINE`` receivers can, by raising.

    A durable receiver declared with ``targets=`` gets one row per target
    instead, and **its callable runs here, inside the caller's transaction**.
    If it raises, this raises, and the caller's change rolls back with the event:
    a fan-out that delivered to nobody would be a committed change whose
    consequences silently never happened. It also costs whatever the callable
    costs, on every event it is owed - one query per fired event per fan-out
    receiver, for one that reads a table.

    A durable receiver declared with ``due=`` has its row written due later:
    ``available_at`` and ``due_at`` at its due time, never earlier than now. A
    ``due`` callable runs here too, on the same terms as ``targets``: if it
    raises, or answers with anything but an aware datetime, this raises.

    The event's declared ``retention`` is copied onto the row. A ``dedupe_key``
    is refused, with ``ValueError``, for an event declared to delete on
    consumption: the key is unique on the row, so it would protect only until
    the prune deleted it.
    """
    # Imported here, not at module level: Django imports an app's package before
    # the app registry is ready, and this package's __init__ re-exports fire().
    #
    # Re-checked after the 2026-09-10 regroup, because moving a module is exactly
    # the kind of change that quietly makes a workaround obsolete and nobody goes
    # back to look. It is still required: __init__ imports delivery.fire at
    # package-import time, so hoisting these two raises ImproperlyConfigured on a
    # bare `import django_domain_events`. Verified by doing it, not by reading.
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

    retention_seconds, delete_when = entry.retention_columns
    if delete_when and dedupe_key is not None:
        # Refused here because it is a call argument, so the declaration
        # cannot see it. The key is unique on the event row, so it protects
        # only while the row exists, and this row goes minutes after it is
        # consumed: a duplicate fired after that would be accepted.
        raise ValueError(
            f"fire({entry.name}) was given a dedupe_key, but {entry.name} is deleted "
            f"once consumed ({entry.retention}), and the key protects only while the "
            f"row exists. Declare it with a timedelta retention to keep the key for a "
            f"known window, or drop the key."
        )

    record = EventRecord.objects.create(
        name=entry.name,
        version=entry.version,
        payload=get_codec().encode(event),
        dedupe_key=dedupe_key,
        # In the project's form, as everything it is compared with is: an aware
        # value under USE_TZ = False is converted to local time, which SQLite
        # would otherwise refuse to store
        # (test_fire_stores_an_aware_occurred_at_as_local_time).
        occurred_at=in_project_time(occurred_at) if occurred_at is not None else timezone.now(),
        actor_id=scope.actor.user_pk,
        actor_key=scope.actor.key,
        actor_label=scope.actor.label,
        # A copy. The stored value is handed to receivers as DeliveryContext.scope
        # and one mutation of a shared dict would reach every later event.
        scope=dict(scope.data),
        correlation_id=scope.correlation_id or inherited_correlation_id(),
        causation_id=causing_event_id(),
        suppressed_reason=suppression[0] if suppression is not None else "",
        # Copied, as max_attempts is onto a delivery, so re-declaring or
        # deleting the class later cannot change this event's fate. At most
        # one of the two is set: see RegisteredEvent.retention_columns.
        retention_seconds=retention_seconds,
        delete_when=delete_when,
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
        actor_label=record.actor_label,
        scope=record.scope,
    )

    receivers = registry.receivers_for(type(event))
    # One entry per row to write. A fan-out receiver's callable runs here, in
    # the caller's transaction, and whatever it raises is not caught: see the
    # docstring for why a failed fan-out must fail the change.
    owed = [
        (r, target)
        for r in receivers
        if r.mode is DeliveryMode.DURABLE
        for target in (
            [""] if r.targets is None else resolve_targets(r.key, r.targets, event, context)
        )
    ]
    # A scheduled receiver's due time, resolved here for the same reason and
    # with the same consequence as a fan-out: in the caller's transaction, and
    # raising into it. Never a fan-out as well, which the declaration refuses,
    # so one time per receiver is one time per row.
    due = {
        r.key: resolve_due(r.key, r.due, event, record.occurred_at, record.recorded_at)
        for r, _ in owed
        if r.due is not None
    }
    if owed:
        rows = DeliveryRecord.objects.bulk_create(
            [
                DeliveryRecord(
                    event=record,
                    receiver_key=r.key,
                    target=target,
                    max_attempts=r.max_attempts,
                    # At the due time for a scheduled receiver, so the claim
                    # takes it then and not before; at once for every other.
                    # due_at stays NULL for those, which reads as recorded_at.
                    available_at=due.get(r.key, record.recorded_at),
                    due_at=due.get(r.key),
                )
                for r, target in owed
            ]
        )
        eager_ids = [row.pk for row, (r, _) in zip(rows, owed, strict=True) if r.eager]
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
        from django_domain_events.delivery.claim_batch import claim_batch
        from django_domain_events.delivery.deliver import dispatch_one

        now = timezone.now()
        claimed = claim_batch(
            worker_id="eager",
            now=now,
            lease=timedelta(seconds=setting("LEASE_SECONDS")),
            limit=len(delivery_ids),
            only_ids=delivery_ids,
        )
        for delivery_id in claimed:
            # As the worker that claimed it. Left to read the owner off the row,
            # the fence would agree with whoever took the row since, and a
            # site="task" hand-off would carry that worker's claim instead.
            dispatch_one(delivery_id, worker_id="eager")

    return run


def call_receiver(
    func: Callable[..., Any], takes_context: bool, event: object, context: DeliveryContext
) -> None:
    """Invoke a receiver with the arity its declaration promised, to completion.

    The one function every execution site passes through - INLINE here in
    ``fire()``, ON_COMMIT through ``_bind``, DURABLE in ``_deliver_one`` - so
    running an ``async def`` receiver here runs it everywhere.
    """
    run_to_completion(func, *((event, context) if takes_context else (event,)))


def run_to_completion(func: Callable[..., Any], *args: object) -> None:
    """Call a sync or an ``async def`` callable and return once it has finished.

    Shared by receivers and their ``on_failure`` hooks. Calling an ``async
    def`` only builds a coroutine, so a caller that discarded what it returned
    recorded the work as done with the body never entered. A coroutine
    function is therefore run through ``async_to_sync``, in this thread's
    stead: outside ASGI on a fresh event loop in a worker thread, and under
    ASGI on the server's loop, while this thread serves
    the coroutine's thread-sensitive ``sync_to_async`` calls - which is what
    keeps Django's async ORM on this thread's connection, inside the
    transaction carrying the acknowledgement. asgiref re-raises whatever the
    coroutine raised, the same object, so ``RetryAfter``, ``NotYet`` and
    ``PermanentFailure`` are read at the boundary exactly as from a function
    (``test_a_verdict_raised_from_a_coroutine_is_read_as_from_a_function``).

    A ``CancelledError`` out of the coroutine is re-raised as a
    ``RuntimeError``, chained to it. It is a ``BaseException``, so every
    ``except Exception`` on the way out would let it through: a durable row
    would stay claimed until its lease lapsed, spend no attempt and take the
    relay down with it, and be retried that way for ever
    (``test_a_coroutine_that_cancels_itself_fails_its_attempt``). Whoever
    cancelled it - the coroutine itself, or, on a server's loop, something
    else running there - it did not finish, which is a failure. The cause is
    kept, so a traceback still shows where it was cancelled
    (``test_an_inline_coroutine_that_cancels_itself_raises_a_chained_runtime_error``).

    A sync callable that returns an awaitable is refused rather than run
    (``test_a_sync_wrapper_returning_a_coroutine_is_refused_and_retried``).
    That is what a ``functools.wraps`` decorator around an ``async def``
    looks like, and a lambda or a factory returning a coroutine: the exact
    shape of the silent drop, and invisible to any detection before the
    call. Not awaited instead, because an awaitable the callable did not
    create fresh - a Task or Future bound to another loop - cannot be awaited
    on this call's loop at all. A coroutine is closed first, so Python's
    "never awaited" warning does not fire beside the error that explains it;
    any other awaitable has nothing to close
    (``test_a_sync_callable_returning_any_awaitable_is_refused``).

    An async generator is refused the same way
    (``test_an_async_generator_is_refused_without_a_warning``): an ``async
    def`` containing ``yield`` is not a coroutine function, so calling it runs
    nothing, and it is not awaitable either, so the check above cannot see it.
    It is left to the garbage collector rather than closed, because a
    generator that never started is dropped without a warning, where an
    ``aclose()`` is itself a coroutine that would need awaiting.
    """
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
    """The callable's qualified name for a message, or its repr where it has none.

    A ``functools.partial`` has no ``__qualname__``; without the fallback the
    message would name ``None``
    (``test_a_refused_callable_with_no_qualified_name_is_named_by_its_repr``).
    """
    return getattr(func, "__qualname__", None) or repr(func)


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
