"""Receivers declared ``async def``, run to completion wherever a receiver runs.

Every test here observes the receiver's body having run, or a refusal having
been recorded, rather than the declaration. Before ``call_receiver`` ran a
coroutine function through ``async_to_sync``, an ``async def`` receiver was
recorded ``SUCCEEDED`` with its body never entered, and the only trace was a
``coroutine ... was never awaited`` warning: so a test asserting a status alone
would pass against exactly that defect.

The tests are plain ``def`` unless they are about a running loop. pytest-asyncio
runs in auto mode, and an ``async def`` test puts a running loop in the thread,
which turns every delivery into the running-loop case.
"""

from __future__ import annotations

import asyncio
import functools
import gc
import logging
import warnings
from collections.abc import AsyncIterator, Callable, Generator, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from unittest import mock

import pytest
from asgiref.sync import markcoroutinefunction, sync_to_async
from django.conf import settings
from django.contrib.auth.models import Group
from django.core.exceptions import SynchronousOnlyOperation
from django.db import connections, transaction
from django.http import HttpRequest, JsonResponse
from django.test import AsyncClient, override_settings
from django.urls import path
from django.utils import timezone

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.deliver import deliver_one, deliver_pending
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.not_yet import NotYet
from django_domain_events.delivery.permanent_failure import PermanentFailure
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import event_registered
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db

NEVER_AWAITED = "never awaited"


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    """Take this file's ad-hoc receivers back out of the process-wide registry.

    Nothing else listens to ``Unheard``, and two unrelated tests assert that, so
    a receiver registered here and left behind would turn them red.
    """
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _owed(key: str) -> DeliveryRecord:
    """Fire one ``Unheard`` and return the row it owes the receiver at ``key``."""
    with transaction.atomic():
        fire(Unheard(value=1))
    return DeliveryRecord.objects.filter(receiver_key=key).latest("pk")


def _delivered(key: str) -> DeliveryRecord:
    """Fire one ``Unheard``, run the delivery it owes ``key`` and return the row."""
    row = _owed(key)
    deliver_one(row.pk)
    row.refresh_from_db()
    return row


def _unawaited(caught: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in caught if NEVER_AWAITED in str(w.message)]


def _sync_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
    """The shape a ``functools.wraps`` decorator gives an ``async def``.

    It looks like an ordinary function to every detection there is, and calling
    it returns the coroutine rather than running it: the exact shape of the
    silent drop.
    """

    @functools.wraps(func)
    def wrapper(*args: Any) -> Any:
        return func(*args)

    return wrapper


# Running at all


def test_an_async_receiver_runs_and_leaves_nothing_unawaited() -> None:
    """Warnings are recorded, not turned into errors. The "never awaited"
    warning is issued from the coroutine's finalizer, where an error filter
    becomes an unraisable-exception report rather than a raise, so a test
    relying on ``filterwarnings("error")`` alone passes against the defect.
    The side effect is what proves the body ran."""
    ran: list[int] = []

    async def remember(event: Unheard) -> None:
        await asyncio.sleep(0)
        ran.append(event.value)

    receiver(Unheard, key="probe.async")(remember)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        row = _delivered("probe.async")
        gc.collect()

    assert ran == [1]
    assert _unawaited(caught) == []
    assert (row.status, row.attempts) == (DeliveryStatus.SUCCEEDED, 1)


def test_each_call_runs_on_a_loop_of_its_own_closed_when_it_returns() -> None:
    """Why a client bound to one loop cannot be shared between calls: the loop
    it was bound to is closed by the time the next call runs. The loops are
    kept, not their ids, because an id can be reused once the first is freed."""
    loops: list[asyncio.AbstractEventLoop] = []

    async def remember_loop(event: Unheard) -> None:
        loops.append(asyncio.get_running_loop())

    receiver(Unheard, key="probe.loop")(remember_loop)
    _delivered("probe.loop")
    _delivered("probe.loop")

    assert len(loops) == 2
    assert loops[0] is not loops[1]
    assert loops[0].is_closed()
    assert loops[1].is_closed()


class _Recorder:
    """A callable object whose ``__call__`` is ``async def``."""

    def __init__(self, ran: list[int]) -> None:
        self.ran = ran

    async def __call__(self, event: Unheard) -> None:
        self.ran.append(event.value)


async def _record_into(ran: list[int], event: Unheard) -> None:
    ran.append(event.value)


def _instance(ran: list[int]) -> Callable[..., Any]:
    return _Recorder(ran)


def _partial(ran: list[int]) -> Callable[..., Any]:
    return functools.partial(_record_into, ran)


def _marked_wrapper(ran: list[int]) -> Callable[..., Any]:
    return markcoroutinefunction(_sync_wrapper(functools.partial(_record_into, ran)))


@pytest.mark.parametrize("shape", [_instance, _partial, _marked_wrapper])
def test_the_async_shapes_detection_accepts_all_run(
    shape: Callable[[list[int]], Callable[..., Any]],
) -> None:
    """A callable object is detected through its type's ``__call__``, a
    partial is unwrapped, and a sync wrapper marked with
    ``markcoroutinefunction`` - the fix the refusal names - is taken at its
    word. The instance case holds the ``__call__`` half of the detection."""
    ran: list[int] = []
    receiver(Unheard, key="probe.shape")(shape(ran))

    row = _delivered("probe.shape")

    assert ran == [1]
    assert (row.status, row.attempts) == (DeliveryStatus.SUCCEEDED, 1)


# The other execution sites


def test_an_async_inline_receiver_runs_before_fire_returns() -> None:
    ran: list[str] = []

    async def inline(event: Unheard) -> None:
        ran.append("inline")

    receiver(Unheard, mode=DeliveryMode.INLINE, key="probe.inline")(inline)

    with transaction.atomic():
        fire(Unheard(value=1))
        assert ran == ["inline"]


def test_an_async_inline_receiver_vetoes_the_change_by_raising() -> None:
    """The receiver's own exception reaches the caller, so the INLINE veto works
    from a coroutine exactly as from a function."""

    class Refused(Exception): ...

    async def veto(event: Unheard) -> None:
        raise Refused("out of stock")

    receiver(Unheard, mode=DeliveryMode.INLINE, key="probe.veto")(veto)

    with pytest.raises(Refused, match="out of stock"), transaction.atomic():
        fire(Unheard(value=1))
    assert not EventRecord.objects.filter(name="testapp.Unheard").exists()


def test_an_async_on_commit_receiver_runs_at_commit(
    django_capture_on_commit_callbacks: Callable[..., Any],
) -> None:
    ran: list[str] = []

    async def after_commit(event: Unheard) -> None:
        ran.append("on_commit")

    receiver(Unheard, mode=DeliveryMode.ON_COMMIT, key="probe.on_commit")(after_commit)

    with django_capture_on_commit_callbacks(execute=True), transaction.atomic():
        fire(Unheard(value=1))
        assert ran == []
    assert ran == ["on_commit"]


def test_an_async_eager_receiver_runs_at_commit(
    django_capture_on_commit_callbacks: Callable[..., Any],
) -> None:
    ran: list[int] = []

    async def eager(event: Unheard) -> None:
        ran.append(event.value)

    receiver(Unheard, key="probe.eager", eager=True)(eager)

    with django_capture_on_commit_callbacks(execute=True), transaction.atomic():
        fire(Unheard(value=1))

    assert ran == [1]
    row = DeliveryRecord.objects.get(receiver_key="probe.eager")
    assert (row.status, row.attempts) == (DeliveryStatus.SUCCEEDED, 1)


# The receiver's transaction, its cause and its clock


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("raises", [False, True], ids=["succeeds", "raises"])
def test_an_async_receivers_writes_share_its_acknowledgements_transaction(raises: bool) -> None:
    """Django's async ORM calls hop back to the calling thread and its
    connection, so what the coroutine writes is in the transaction carrying the
    acknowledgement: kept with a success, rolled back with a raise. A real
    transaction (``transaction=True``) rather than the test's savepoint, so the
    delivery's ``atomic()`` is the outermost one, as in a relay."""
    seen: list[bool] = []

    async def create_group(event: Unheard) -> None:
        await Group.objects.acreate(name="probe-async-write")
        seen.append(await Group.objects.filter(name="probe-async-write").aexists())
        if raises:
            raise RuntimeError("after the write")

    receiver(Unheard, key="probe.write")(create_group)

    row = _delivered("probe.write")

    assert seen == [True]
    kept = Group.objects.filter(name="probe-async-write").exists()
    if raises:
        assert (row.status, row.attempts, kept) == (DeliveryStatus.FAILED, 1, False)
        assert row.last_error == "RuntimeError: after the write"
    else:
        assert (row.status, row.attempts, kept) == (DeliveryStatus.SUCCEEDED, 1, True)


@dataclass(frozen=True)
class _Child:
    value: int


def test_an_event_fired_inside_an_async_receiver_records_its_parent() -> None:
    """The cause is a ContextVar, and asgiref copies the calling context into the
    coroutine and on into ``sync_to_async``, so nothing is threaded through."""

    async def fire_child(event: Unheard) -> None:
        await sync_to_async(fire)(_Child(value=event.value))

    receiver(Unheard, key="probe.parent")(fire_child)

    with event_registered(_Child, "probe.Child"):
        row = _delivered("probe.parent")
        children = EventRecord.objects.filter(name="probe.Child")

        assert row.status == DeliveryStatus.SUCCEEDED
        assert list(children.values_list("causation_id", "correlation_id")) == [
            (row.event.pk, row.event.correlation_id)
        ]


def test_an_async_receiver_sees_the_clock_and_due_time_a_sync_one_sees() -> None:
    """Run by CI under both ``USE_TZ`` values and a zone other than UTC. The
    clock is read the same way on the loop's thread, the context carries the
    same ``due_at``, and an activated zone is visible inside the coroutine."""
    seen: dict[str, tuple[Any, ...]] = {}

    def sync_probe(event: Unheard, ctx: DeliveryContext) -> None:
        seen["sync"] = (ctx.due_at, timezone.now(), timezone.get_current_timezone_name())

    async def async_probe(event: Unheard, ctx: DeliveryContext) -> None:
        seen["async"] = (ctx.due_at, timezone.now(), timezone.get_current_timezone_name())

    receiver(Unheard, key="probe.sync_clock", takes_context=True)(sync_probe)
    receiver(Unheard, key="probe.async_clock", takes_context=True)(async_probe)

    with transaction.atomic():
        fire(Unheard(value=1))
    before = timezone.now()
    with timezone.override("Asia/Kolkata"):
        for row in DeliveryRecord.objects.filter(receiver_key__startswith="probe."):
            deliver_one(row.pk)
    after = timezone.now()

    assert set(seen) == {"sync", "async"}
    sync_due, sync_now, sync_zone = seen["sync"]
    async_due, async_now, async_zone = seen["async"]
    assert async_due == sync_due
    assert async_zone == sync_zone == "Asia/Kolkata"
    assert (async_now.tzinfo is None, sync_now.tzinfo is None) == (not settings.USE_TZ,) * 2
    assert before <= async_now <= after


# The verdicts a receiver can raise


def _retry_after() -> Exception:
    return RetryAfter(seconds=60, reason="the endpoint sent Retry-After: 60")


def _not_yet() -> Exception:
    return NotYet(timezone.now() + timedelta(hours=1), reason="not due")


def _permanent() -> Exception:
    return PermanentFailure("410 Gone")


def _raising(verdict: Callable[[], Exception], is_async: bool) -> Callable[..., Any]:
    if is_async:

        async def coroutine(event: Unheard) -> None:
            raise verdict()

        return coroutine

    def function(event: Unheard) -> None:
        raise verdict()

    return function


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("verdict", "status", "attempts"),
    [
        (_retry_after, DeliveryStatus.FAILED, 1),
        (_not_yet, DeliveryStatus.PENDING, 0),
        (_permanent, DeliveryStatus.DEAD, 1),
    ],
    ids=["retry_after", "not_yet", "permanent_failure"],
)
def test_a_verdict_raised_from_a_coroutine_is_read_as_from_a_function(
    verdict: Callable[[], Exception], status: DeliveryStatus, attempts: int, is_async: bool
) -> None:
    """asgiref re-raises the coroutine's own exception object, so the boundary
    reading verdicts sees the same type and the same fields. Each case asserts
    the same row whichever way the receiver is declared."""
    receiver(Unheard, key="probe.verdict")(_raising(verdict, is_async))
    row = _owed("probe.verdict")

    before = timezone.now()
    with mock.patch("django_domain_events.delivery.deliver.random.random", return_value=0.0):
        deliver_one(row.pk)
    after = timezone.now()
    row.refresh_from_db()

    assert (row.status, row.attempts) == (status, attempts)
    if verdict is _retry_after:
        assert before + timedelta(seconds=60) <= row.available_at <= after + timedelta(seconds=60)
    elif verdict is _not_yet:
        assert before + timedelta(hours=1) <= row.available_at <= after + timedelta(hours=1)
        assert row.due_at == row.available_at


# What the coroutine cannot do


async def _sync_orm(event: Unheard) -> None:
    Group.objects.create(name="probe-sync-orm")


async def _atomic(event: Unheard) -> None:
    with transaction.atomic():
        await Group.objects.acreate(name="probe-atomic")


@pytest.mark.parametrize("body", [_sync_orm, _atomic], ids=["sync_orm", "atomic"])
def test_a_sync_database_call_in_a_coroutine_fails_the_attempt_loudly(
    body: Callable[..., Any],
) -> None:
    """Django refuses a sync database call on the loop's thread. That covers a
    sync ORM call and ``transaction.atomic()``, which is a sync API: a receiver
    wanting a savepoint wraps a sync function in ``sync_to_async``. Loud, not
    corrupting, and an ordinary failure."""
    receiver(Unheard, key="probe.sync_call")(body)

    row = _delivered("probe.sync_call")

    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error.startswith("SynchronousOnlyOperation: ")
    assert not Group.objects.filter(name__startswith="probe-").exists()


# Refused at the call


def test_a_sync_wrapper_returning_a_coroutine_is_refused_and_retried() -> None:
    """Refused rather than run, with the fix named, and closed so no "never
    awaited" warning fires. An ordinary failure, so the row is retried and a
    deploy that fixes the declaration drains it without ``requeue_dead``."""
    ran: list[int] = []

    async def body(event: Unheard) -> None:
        ran.append(event.value)

    receiver(Unheard, key="probe.wrapped")(_sync_wrapper(body))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        row = _delivered("probe.wrapped")
        gc.collect()

    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error.startswith("TypeError: ")
    assert "async def" in row.last_error
    assert "markcoroutinefunction" in row.last_error
    assert ran == []
    assert _unawaited(caught) == []


class _Awaitable:
    """An awaitable that is not a coroutine, such as a Future: nothing to close."""

    def __await__(self) -> Generator[None, None, None]:
        yield


def test_a_sync_callable_returning_any_awaitable_is_refused() -> None:
    def returns_awaitable(event: Unheard) -> _Awaitable:
        return _Awaitable()

    receiver(Unheard, key="probe.awaitable")(returns_awaitable)

    row = _delivered("probe.awaitable")

    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error.startswith("TypeError: ")
    assert "_Awaitable" in row.last_error


def test_an_inline_sync_wrapper_returning_a_coroutine_raises_into_the_caller() -> None:
    async def body(event: Unheard) -> None: ...

    receiver(Unheard, mode=DeliveryMode.INLINE, key="probe.inline_wrapped")(_sync_wrapper(body))

    with pytest.raises(TypeError, match="markcoroutinefunction"), transaction.atomic():
        fire(Unheard(value=1))


@pytest.mark.filterwarnings("ignore:coroutine .* was never awaited")
@pytest.mark.parametrize("name", ["targets", "due"])
def test_a_sync_wrapper_around_an_async_targets_or_due_fails_at_the_fire(name: str) -> None:
    """The decorator refuses an ``async def`` for either, and cannot see one
    behind a sync wrapper. That still fails loudly, inside the caller's
    transaction, because a coroutine is neither a list of targets nor a
    datetime. The coroutine is not this package's to close here, so the
    warning is expected."""

    async def resolver(*args: object) -> None: ...

    async def body(event: Unheard) -> None: ...

    receiver(Unheard, key="probe.wrapped_resolver", **{name: _sync_wrapper(resolver)})(body)

    with pytest.raises(TypeError, match="coroutine"), transaction.atomic():
        fire(Unheard(value=1))
    assert not EventRecord.objects.filter(name="testapp.Unheard").exists()


# Cancelled


def test_a_coroutine_that_cancels_itself_fails_its_attempt() -> None:
    """``CancelledError`` is a ``BaseException``, so the relay's ``except
    Exception`` would let it through: the row would stay claimed until its
    lease lapsed, spend no attempt, and take the relay down. Read as what it
    is - the receiver did not finish - it is an ordinary failure. Run through
    ``deliver_pending`` so the row is claimed first, which is the state it
    would otherwise be stranded in."""

    async def gives_up(event: Unheard) -> None:
        task = asyncio.current_task()
        assert task is not None
        task.cancel()
        await asyncio.sleep(0)

    receiver(Unheard, key="probe.cancelled")(gives_up)
    with transaction.atomic():
        fire(Unheard(value=1))

    assert deliver_pending(worker_id="probe") == {DeliveryStatus.FAILED: 1}
    row = DeliveryRecord.objects.get(receiver_key="probe.cancelled")
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error.startswith("RuntimeError: ")
    assert "gives_up was cancelled" in row.last_error


# The failure hook


def _failing(event: Unheard) -> None:
    raise RuntimeError("the endpoint said no")


def test_an_async_on_failure_hook_runs() -> None:
    seen: list[DeliveryFailure] = []

    async def remember(failure: DeliveryFailure) -> None:
        await asyncio.sleep(0)
        seen.append(failure)

    receiver(Unheard, key="probe.async_hook", on_failure=remember)(_failing)

    row = _delivered("probe.async_hook")

    assert [(f.receiver_key, f.attempt, f.status, f.error) for f in seen] == [
        ("probe.async_hook", 1, DeliveryStatus.FAILED, "RuntimeError: the endpoint said no")
    ]
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)


class _HookBroke(Exception): ...


async def _raising_hook(failure: DeliveryFailure) -> None:
    raise _HookBroke("the log table is gone")


async def _cancelled_hook(failure: DeliveryFailure) -> None:
    raise asyncio.CancelledError


@pytest.mark.parametrize(
    ("hook", "logged"),
    [
        (_raising_hook, _HookBroke),
        (_cancelled_hook, RuntimeError),
        (_sync_wrapper(_raising_hook), TypeError),
    ],
    ids=["raises", "cancelled", "sync_wrapper"],
)
def test_an_async_on_failure_hook_that_fails_is_logged_and_swallowed(
    hook: Callable[..., Any], logged: type[BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    """As a sync hook's raise is. A cancelled one arrives as the adapter's
    RuntimeError, which is an ``Exception``; left a ``CancelledError`` it would
    escape the hook's ``except Exception`` and the delivery with it."""
    receiver(Unheard, key="probe.broken_hook", on_failure=hook)(_failing)

    with caplog.at_level(logging.ERROR, logger="django_domain_events.delivery.deliver"):
        row = _delivered("probe.broken_hook")

    hook_records = [r for r in caplog.records if "on_failure hook" in r.getMessage()]
    assert [r.exc_info[0] if r.exc_info else None for r in hook_records] == [logged]
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)


# Inside a running loop


@pytest.fixture
async def _thread_connections_closed() -> AsyncIterator[None]:
    """Close what ``sync_to_async`` opened on asgiref's shared worker thread.

    Called from a test whose loop runs in the main thread, a thread-sensitive
    ``sync_to_async`` - the ORM's, and the one Django serves a sync view
    through - runs on one long-lived worker thread, which keeps its database
    connection open after the test. On Postgres that open session stops the
    test database being dropped at the end of the run.
    """
    yield
    await sync_to_async(connections.close_all)()


def _owed_pk(key: str) -> int:
    return _owed(key).pk


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("_thread_connections_closed")
async def test_deliver_one_inside_a_running_loop_stops_before_the_receiver() -> None:
    """A pin of a limit rather than a test of the change: the row is read
    through the sync ORM before any receiver is reached, so the call raises
    ``SynchronousOnlyOperation`` with the row untouched and no attempt spent."""

    async def body(event: Unheard) -> None: ...

    receiver(Unheard, key="probe.in_loop")(body)
    pk = await sync_to_async(_owed_pk)("probe.in_loop")

    with pytest.raises(SynchronousOnlyOperation):
        deliver_one(pk)

    row = await DeliveryRecord.objects.aget(pk=pk)
    assert (row.status, row.attempts) == (DeliveryStatus.PENDING, 0)


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("_thread_connections_closed")
async def test_deliver_one_allowed_inside_a_running_loop_records_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With ``DJANGO_ALLOW_ASYNC_UNSAFE`` the call reaches the receiver, and
    asgiref refuses to start a loop in a thread that has one. That is an
    ordinary failure, and spends an attempt."""
    ran: list[int] = []

    async def body(event: Unheard) -> None:
        ran.append(event.value)

    receiver(Unheard, key="probe.unsafe")(body)
    pk = await sync_to_async(_owed_pk)("probe.unsafe")
    monkeypatch.setenv("DJANGO_ALLOW_ASYNC_UNSAFE", "true")

    assert deliver_one(pk) == DeliveryStatus.FAILED

    row = DeliveryRecord.objects.get(pk=pk)
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error.startswith("RuntimeError: You cannot use AsyncToSync")
    assert ran == []
    # Otherwise the connection used from inside the loop is still open when
    # Postgres is asked to drop the test database. Closed while allowed.
    connections.close_all()


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("_thread_connections_closed")
@pytest.mark.filterwarnings("ignore:fire\\(testapp.Unheard\\) ran outside a transaction")
async def test_fire_inside_a_running_loop_never_reaches_an_inline_receiver_unless_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``fire()`` writes before it reaches a receiver, so in a running loop it
    stops at the write. Allowed past that, the INLINE async receiver is where
    asgiref refuses, and its RuntimeError reaches the caller."""

    async def body(event: Unheard) -> None: ...

    receiver(Unheard, mode=DeliveryMode.INLINE, key="probe.loop_inline")(body)

    with pytest.raises(SynchronousOnlyOperation):
        fire(Unheard(value=1))

    monkeypatch.setenv("DJANGO_ALLOW_ASYNC_UNSAFE", "true")
    with pytest.raises(RuntimeError, match="You cannot use AsyncToSync"), transaction.atomic():
        fire(Unheard(value=1))
    assert not EventRecord.objects.filter(name="testapp.Unheard").exists()
    # Otherwise the connection used from inside the loop is still open when
    # Postgres is asked to drop the test database. Closed while allowed.
    connections.close_all()


# Under ASGI


class _Undo(Exception): ...


def _fire_in_a_request(request: HttpRequest) -> JsonResponse:
    """A sync view: Django serves it on a thread under ``sync_to_async``."""
    seen = False
    try:
        with transaction.atomic():
            fire(Unheard(value=2))
            seen = Group.objects.filter(name="probe-asgi").exists()
            raise _Undo
    except _Undo:
        ...
    return JsonResponse({"seen": seen})


def _fire_and_commit(request: HttpRequest) -> JsonResponse:
    with transaction.atomic():
        fire(Unheard(value=3))
    return JsonResponse({})


urlpatterns = [path("fire/", _fire_in_a_request), path("commit/", _fire_and_commit)]


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("_thread_connections_closed")
async def test_under_asgi_an_inline_coroutine_writes_in_the_requests_transaction() -> None:
    """Served through Django's ASGI handler, asgiref schedules the coroutine on
    the server's loop rather than a fresh one, and its ORM calls still hop back
    to the request's thread: the row is visible in the request's transaction
    and rolled back with it."""
    loops: list[asyncio.AbstractEventLoop] = []

    async def create_group(event: Unheard) -> None:
        loops.append(asyncio.get_running_loop())
        await Group.objects.acreate(name="probe-asgi")

    receiver(Unheard, mode=DeliveryMode.INLINE, key="probe.asgi")(create_group)

    with override_settings(ROOT_URLCONF=__name__):
        response = await AsyncClient().get("/fire/")

    assert response.status_code == 200
    assert response.json() == {"seen": True}
    assert loops == [asyncio.get_running_loop()]
    assert not await Group.objects.filter(name="probe-asgi").aexists()
    assert not await EventRecord.objects.filter(name="testapp.Unheard").aexists()


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("_thread_connections_closed")
async def test_under_asgi_an_on_commit_coroutine_runs_on_the_servers_loop() -> None:
    """Commit happens on the request's thread too, so an ON_COMMIT coroutine is
    scheduled where an INLINE one is: why a client shared between calls can
    work under ASGI and then fail in the relay."""
    loops: list[asyncio.AbstractEventLoop] = []

    async def remember_loop(event: Unheard) -> None:
        loops.append(asyncio.get_running_loop())

    receiver(Unheard, mode=DeliveryMode.ON_COMMIT, key="probe.asgi_commit")(remember_loop)

    with override_settings(ROOT_URLCONF=__name__):
        response = await AsyncClient().get("/commit/")

    assert response.status_code == 200
    assert loops == [asyncio.get_running_loop()]
