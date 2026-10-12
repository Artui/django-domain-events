"""``AROUND_DELIVERY``: one context manager around each durable attempt.

Every test here configures the hook by its dotted path, as a project does, and
observes what it recorded: when it was entered, with which context, and what
``__exit__`` was handed. The hooks are factories in this module, so
``import_string`` resolves them exactly as it would a project's own.

A test asserting only that something did *not* happen would pass against a
tree with no hook at all. The negative ones therefore deliver a control row
too, and assert the hook was entered for that one and for nothing else.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextvars import ContextVar
from datetime import timedelta
from typing import Any

import pytest
from django.contrib.auth.models import Group
from django.db import transaction
from django.utils import timezone

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_one, deliver_pending, dispatch_one
from django_domain_events.delivery.django_tasks_backend import deliver_delivery
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.not_yet import NotYet
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db

#: What every probe hook recorded, in order: ``("enter", context)`` and
#: ``("exit", exc_type, exc, traceback)``.
EVENTS: list[tuple[Any, ...]] = []

#: Set by the probe hook for the length of the attempt, as a log context hook
#: would set a request id.
CURRENT: ContextVar[str | None] = ContextVar("CURRENT", default=None)


class _Stop(BaseException):
    """Stands in for a worker's timeout signal: not an ``Exception``."""


class _Probe:
    """A context manager recording how it was driven."""

    def __init__(
        self,
        context: DeliveryContext,
        *,
        enter_raises: BaseException | None = None,
        exit_raises: BaseException | None = None,
        exit_returns: bool | None = None,
    ) -> None:
        self.context = context
        self.enter_raises = enter_raises
        self.exit_raises = exit_raises
        self.exit_returns = exit_returns

    def __enter__(self) -> None:
        EVENTS.append(("enter", self.context))
        if self.enter_raises is not None:
            raise self.enter_raises
        self.token = CURRENT.set(
            f"{self.context.delivery_id}:{getattr(self.context, 'receiver_key', None)}"
        )

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        EVENTS.append(("exit", exc_type, exc, tb))
        CURRENT.reset(self.token)
        if self.exit_raises is not None:
            raise self.exit_raises
        return self.exit_returns


def recording(context: DeliveryContext) -> _Probe:
    return _Probe(context)


def other_recording(context: DeliveryContext) -> _Probe:
    """A second hook, to tell which of two settings a delivery read."""
    EVENTS.append(("other", context))
    return _Probe(context)


def suppressing(context: DeliveryContext) -> _Probe:
    return _Probe(context, exit_returns=True)


def raising_on_exit(context: DeliveryContext) -> _Probe:
    return _Probe(context, exit_raises=RuntimeError("the exporter is down"))


def raising_on_enter(context: DeliveryContext) -> _Probe:
    return _Probe(context, enter_raises=RuntimeError("the tracer is down"))


def not_yet_on_enter(context: DeliveryContext) -> _Probe:
    return _Probe(
        context, enter_raises=NotYet(timezone.now() + timedelta(hours=1), reason="tenant paused")
    )


def retry_after_on_enter(context: DeliveryContext) -> _Probe:
    return _Probe(context, enter_raises=RetryAfter(60, counts=False))


class _Writes(_Probe):
    """Records the attempt in the database on entering, as an audit hook would."""

    def __enter__(self) -> None:
        super().__enter__()
        Group.objects.create(name=f"probe-attempt-{self.context.delivery_id}")


def writing(context: DeliveryContext) -> _Probe:
    return _Writes(context)


def not_a_context_manager(context: DeliveryContext) -> None:
    EVENTS.append(("factory", context))


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    """Clear the record, and take this file's receivers back out of the registry.

    Nothing else listens to ``Unheard``, and two unrelated tests assert that.
    """
    EVENTS.clear()
    yield
    EVENTS.clear()
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _hook(settings: Any, name: str | None) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {
        "AROUND_DELIVERY": None if name is None else f"{__name__}.{name}"
    }


def _fire() -> int:
    with transaction.atomic():
        event_id = fire(Unheard(value=1))
    assert event_id is not None
    return event_id


def _row(event_id: int, key: str) -> DeliveryRecord:
    return DeliveryRecord.objects.get(event_id=event_id, receiver_key=key)


def _owed(key: str) -> DeliveryRecord:
    return _row(_fire(), key)


def _entered() -> list[DeliveryContext]:
    return [entry[1] for entry in EVENTS if entry[0] == "enter"]


def _exits() -> list[tuple[Any, ...]]:
    return [entry[1:] for entry in EVENTS if entry[0] == "exit"]


def _record(ran: list[Any]) -> Callable[[Unheard], None]:
    def remember(event: Unheard) -> None:
        ran.append(CURRENT.get())

    return remember


def _assert_wrapped_once(row: DeliveryRecord, key: str) -> None:
    entered = _entered()
    assert len(entered) == 1
    context = entered[0]
    assert context.delivery_id == row.pk
    assert context.event_id == row.event.pk
    assert context.attempt == 1
    assert getattr(context, "receiver_key", None) == key
    assert _exits() == [(None, None, None)]


# Where it runs: every durable site, once per attempt


def _by_deliver_one(row: DeliveryRecord) -> None:
    deliver_one(row.pk)


def _by_deliver_pending(row: DeliveryRecord) -> None:
    deliver_pending(worker_id="w1")


def _by_drain_outbox(row: DeliveryRecord) -> None:
    drain_outbox()


def _by_the_task_body(row: DeliveryRecord) -> None:
    """What a task worker runs, with the claim the relay's message carries."""
    claim_batch(worker_id="w1", now=timezone.now(), lease=timedelta(minutes=5), limit=10)
    row.refresh_from_db()
    assert row.claimed_at is not None
    deliver_delivery(row.pk, "w1", row.claimed_at.isoformat())


@pytest.mark.parametrize(
    "deliver",
    [_by_deliver_one, _by_deliver_pending, _by_drain_outbox, _by_the_task_body],
    ids=["deliver_one", "deliver_pending", "drain_outbox", "task"],
)
def test_the_hook_wraps_each_attempt_once_with_its_context(
    settings: Any, deliver: Callable[[DeliveryRecord], None]
) -> None:
    ran: list[Any] = []
    receiver(Unheard, key="probe.durable")(_record(ran))
    _hook(settings, "recording")
    row = _owed("probe.durable")

    deliver(row)

    row.refresh_from_db()
    assert row.status == DeliveryStatus.SUCCEEDED
    _assert_wrapped_once(row, "probe.durable")
    # The receiver ran inside it: the value the hook set is the one it read.
    assert ran == [f"{row.pk}:probe.durable"]


@pytest.mark.django_db(transaction=True)
def test_the_relay_wraps_each_attempt(settings: Any) -> None:
    """A real transaction, as the relay's tests use, so its claim and each
    delivery's ``atomic()`` are the outermost ones."""
    ran: list[Any] = []
    receiver(Unheard, key="probe.relayed")(_record(ran))
    _hook(settings, "recording")
    row = _owed("probe.relayed")

    assert run_relay(worker_id="w1", passes=1, allow_unsafe_concurrency=True) == {
        DeliveryStatus.SUCCEEDED: 1
    }

    _assert_wrapped_once(row, "probe.relayed")
    assert ran == [f"{row.pk}:probe.relayed"]


def test_the_eager_attempt_is_wrapped(
    settings: Any, django_capture_on_commit_callbacks: Callable[..., Any]
) -> None:
    ran: list[Any] = []
    receiver(Unheard, key="probe.eager", eager=True)(_record(ran))
    _hook(settings, "recording")

    with django_capture_on_commit_callbacks(execute=True), transaction.atomic():
        event_id = fire(Unheard(value=1))

    assert event_id is not None
    row = _row(event_id, "probe.eager")
    assert row.status == DeliveryStatus.SUCCEEDED
    _assert_wrapped_once(row, "probe.eager")


def test_a_row_handed_to_a_task_is_wrapped_once_in_the_worker_and_not_at_the_hand_off(
    settings: Any,
) -> None:
    """The relay's hand-off never runs the receiver, so wrapping it too would
    open a span for a delivery that has not happened, and a second one when it
    does. The immediate backend runs the task inside ``dispatch_one``, so one
    call covers both halves."""
    settings.TASKS = {"default": {"BACKEND": "django_tasks.backends.immediate.ImmediateBackend"}}
    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": "django_domain_events.delivery.django_tasks_backend.DjangoTasksBackend",
        "AROUND_DELIVERY": f"{__name__}.recording",
    }
    ran: list[Any] = []
    receiver(Unheard, key="probe.task", site="task")(_record(ran))
    row = _owed("probe.task")
    claim_batch(worker_id="w1", now=timezone.now(), lease=timedelta(minutes=5), limit=10)

    assert dispatch_one(row.pk, worker_id="w1") is None

    row.refresh_from_db()
    assert row.status == DeliveryStatus.SUCCEEDED
    _assert_wrapped_once(row, "probe.task")


def test_the_path_is_read_on_every_delivery(settings: Any) -> None:
    """Not cached, for the reason ``get_codec`` gives: a cached hook would be
    whatever the first delivery saw."""
    receiver(Unheard, key="probe.durable")(lambda event: None)
    first, second = _owed("probe.durable"), _owed("probe.durable")

    _hook(settings, "recording")
    deliver_one(first.pk)
    _hook(settings, "other_recording")
    deliver_one(second.pk)

    assert [(entry[0], entry[1].delivery_id) for entry in EVENTS if entry[0] != "exit"] == [
        ("enter", first.pk),
        ("other", second.pk),
        ("enter", second.pk),
    ]


def test_with_no_hook_a_delivery_runs_as_before(settings: Any) -> None:
    ran: list[Any] = []
    receiver(Unheard, key="probe.durable")(_record(ran))
    _hook(settings, None)
    row = _owed("probe.durable")

    deliver_one(row.pk)

    row.refresh_from_db()
    assert row.status == DeliveryStatus.SUCCEEDED
    assert ran == [None]
    assert EVENTS == []


# Exit: after the outcome is recorded, never able to change it


def test_exit_sees_the_receivers_exception_and_cannot_suppress_it(settings: Any) -> None:
    """``__exit__`` returning True would suppress the exception in a ``with``
    statement. Here the failure is recorded before ``__exit__`` is called, so
    there is nothing left for it to suppress, and its answer is ignored."""
    failure = ValueError("the mail server refused")

    def fails(event: Unheard) -> None:
        raise failure

    receiver(Unheard, key="probe.fails")(fails)
    _hook(settings, "suppressing")
    row = _owed("probe.fails")

    assert deliver_one(row.pk) == DeliveryStatus.FAILED

    row.refresh_from_db()
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error == "ValueError: the mail server refused"
    assert [(exc_type, exc) for exc_type, exc, _ in _exits()] == [(ValueError, failure)]
    assert _exits()[0][2] is not None, "and the traceback, as a with statement passes it"


def test_exit_raising_after_a_success_leaves_the_acknowledgement_and_is_logged(
    settings: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """The acknowledgement has committed before ``__exit__`` runs. Run inside
    the receiver's transaction, the raise would roll back the acknowledgement of
    work already done; reaching the failure path, it would overwrite SUCCEEDED
    with FAILED and the work would be done again."""

    def writes(event: Unheard) -> None:
        Group.objects.create(name="probe-around-delivery")

    receiver(Unheard, key="probe.writes")(writes)
    _hook(settings, "raising_on_exit")
    row = _owed("probe.writes")

    with caplog.at_level(logging.ERROR, logger="django_domain_events.delivery.deliver"):
        assert deliver_one(row.pk) == DeliveryStatus.SUCCEEDED

    row.refresh_from_db()
    assert (row.status, row.attempts, row.last_error) == (DeliveryStatus.SUCCEEDED, 1, "")
    assert Group.objects.filter(name="probe-around-delivery").exists()
    logged_lines = [r for r in caplog.records if "AROUND_DELIVERY" in r.getMessage()]
    assert len(logged_lines) == 1
    logged = logged_lines[0]
    assert logged.levelno == logging.ERROR
    assert "probe.writes" in logged.getMessage()
    assert str(row.pk) in logged.getMessage()
    assert logged.exc_info is not None
    assert str(logged.exc_info[1]) == "the exporter is down"


def test_exit_raising_after_a_failure_is_logged_and_the_failure_stands(
    settings: Any, caplog: pytest.LogCaptureFixture
) -> None:
    def fails(event: Unheard) -> None:
        raise ValueError("refused")

    receiver(Unheard, key="probe.fails")(fails)
    _hook(settings, "raising_on_exit")
    row = _owed("probe.fails")

    with caplog.at_level(logging.ERROR, logger="django_domain_events.delivery.deliver"):
        assert deliver_one(row.pk) == DeliveryStatus.FAILED

    row.refresh_from_db()
    assert (row.status, row.attempts, row.last_error) == (
        DeliveryStatus.FAILED,
        1,
        "ValueError: refused",
    )
    assert [r.levelno for r in caplog.records if "AROUND_DELIVERY" in r.getMessage()] == [
        logging.ERROR
    ]


def test_a_row_lost_mid_flight_still_exits_the_hook(settings: Any) -> None:
    """The receiver finishes after another worker took the row: its work rolls
    back, nothing is recorded, and the hook is still closed, cleanly, since the
    receiver did not raise. A relay thread lives for days, so a hook left open
    leaves its log context on every later line."""
    seen: list[Any] = []

    def loses_the_row(event: Unheard) -> None:
        DeliveryRecord.objects.filter(receiver_key="probe.loses").update(claimed_by="w2")

    receiver(Unheard, key="probe.loses")(loses_the_row)
    _hook(settings, "recording")
    row = _owed("probe.loses")
    claim_batch(worker_id="w1", now=timezone.now(), lease=timedelta(minutes=5), limit=10)

    assert deliver_one(row.pk, worker_id="w1") is None

    seen.append(CURRENT.get())
    assert seen == [None]
    assert len(_entered()) == 1
    assert _exits() == [(None, None, None)]


def test_the_hook_is_exited_even_when_recording_the_failure_raises(
    settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed write of the outcome (the database went away) propagates, as it
    did before the hook existed. The hook is still exited, with the receiver's
    exception, so a span closes and a log context is reset."""
    failure = ValueError("refused")

    def fails(event: Unheard) -> None:
        raise failure

    def write_fails(*args: Any, **kwargs: Any) -> None:
        raise ConnectionError("the database went away")

    receiver(Unheard, key="probe.fails")(fails)
    _hook(settings, "recording")
    row = _owed("probe.fails")
    monkeypatch.setattr(deliver_module, "_fail", write_fails)

    with pytest.raises(ConnectionError):
        deliver_one(row.pk)

    assert [(exc_type, exc) for exc_type, exc, _ in _exits()] == [(ValueError, failure)]
    assert CURRENT.get() is None


def test_a_base_exception_reaches_exit_and_propagates(settings: Any) -> None:
    """A worker's timeout signal or a KeyboardInterrupt is not a failure of the
    delivery. The hook sees it, the row stays claimed until its lease lapses,
    and it goes on up."""
    stop = _Stop()

    def interrupted(event: Unheard) -> None:
        raise stop

    receiver(Unheard, key="probe.stopped")(interrupted)
    _hook(settings, "recording")
    row = _owed("probe.stopped")
    claim_batch(worker_id="w1", now=timezone.now(), lease=timedelta(minutes=5), limit=10)

    with pytest.raises(_Stop):
        deliver_one(row.pk, worker_id="w1")

    row.refresh_from_db()
    assert (row.status, row.attempts) == (DeliveryStatus.CLAIMED, 0)
    assert [(exc_type, exc) for exc_type, exc, _ in _exits()] == [(_Stop, stop)]


def test_what_the_hook_writes_is_not_rolled_back_with_the_receiver(settings: Any) -> None:
    """The hook is outside the transaction carrying the acknowledgement, so a
    receiver raising rolls back its own work and not the hook's."""

    def fails(event: Unheard) -> None:
        Group.objects.create(name="probe-receiver-work")
        raise ValueError("refused")

    receiver(Unheard, key="probe.fails")(fails)
    _hook(settings, "writing")
    row = _owed("probe.fails")

    assert deliver_one(row.pk) == DeliveryStatus.FAILED

    assert not Group.objects.filter(name="probe-receiver-work").exists()
    assert Group.objects.filter(name=f"probe-attempt-{row.pk}").exists()


# Enter: raising there fails the attempt, read as the receiver's own raise


def test_entering_raising_fails_the_attempt_without_running_the_receiver(settings: Any) -> None:
    """A hook setting a tenant or a database role that cannot be set must not
    let the receiver run without it."""
    ran: list[Any] = []
    told: list[DeliveryFailure] = []
    receiver(Unheard, key="probe.guarded", on_failure=told.append)(_record(ran))
    _hook(settings, "raising_on_enter")
    row = _owed("probe.guarded")

    assert deliver_one(row.pk) == DeliveryStatus.FAILED

    row.refresh_from_db()
    assert ran == []
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error == "RuntimeError: the tracer is down"
    assert [(f.delivery_id, f.attempt, f.error) for f in told] == [
        (row.pk, 1, "RuntimeError: the tracer is down")
    ]
    # Entered, never exited: a context manager whose __enter__ raised was
    # never entered, which is what a with statement does too.
    assert len(_entered()) == 1
    assert _exits() == []


def test_a_hook_entering_with_not_yet_defers_the_delivery(settings: Any) -> None:
    ran: list[Any] = []
    receiver(Unheard, key="probe.paused")(_record(ran))
    _hook(settings, "not_yet_on_enter")
    row = _owed("probe.paused")
    before = timezone.now()

    assert deliver_one(row.pk) == DeliveryStatus.PENDING

    row.refresh_from_db()
    assert ran == []
    assert (row.status, row.attempts) == (DeliveryStatus.PENDING, 0)
    assert row.available_at > before + timedelta(minutes=59)


def test_a_hook_entering_with_an_uncounted_retry_after_defers_without_spending(
    settings: Any,
) -> None:
    ran: list[Any] = []
    receiver(Unheard, key="probe.paused", give_up_after=timedelta(days=1))(_record(ran))
    _hook(settings, "retry_after_on_enter")
    row = _owed("probe.paused")

    deliver_one(row.pk)

    row.refresh_from_db()
    assert ran == []
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 0)


@pytest.mark.parametrize(
    ("name", "error"),
    [("missing_hook", "ImportError"), ("not_a_context_manager", "AttributeError")],
    ids=["path-does-not-import", "factory-returns-no-context-manager"],
)
def test_a_hook_that_cannot_be_entered_at_all_fails_the_attempt(
    settings: Any, name: str, error: str
) -> None:
    """Resolving the path and calling the factory are part of entering. A typo
    the system check would have named, deployed anyway, fails each attempt
    loudly rather than taking the relay down or running receivers unwrapped."""
    ran: list[Any] = []
    receiver(Unheard, key="probe.guarded")(_record(ran))
    _hook(settings, name)
    row = _owed("probe.guarded")

    assert deliver_one(row.pk) == DeliveryStatus.FAILED

    row.refresh_from_db()
    assert ran == []
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert row.last_error.startswith(f"{error}: ")


# What runs inside it


def test_on_failure_and_the_failure_warning_run_inside_the_hook(
    settings: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Most of what a log context hook is for: the lines a failure writes carry
    the context the hook set."""
    told: list[Any] = []

    def fails(event: Unheard) -> None:
        raise ValueError("refused")

    def on_failure(failure: DeliveryFailure) -> None:
        told.append(CURRENT.get())

    class _Stamp(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.current = CURRENT.get()
            return True

    receiver(Unheard, key="probe.fails", on_failure=on_failure)(fails)
    _hook(settings, "recording")
    row = _owed("probe.fails")
    stamp = _Stamp()
    deliver_logger = logging.getLogger("django_domain_events.delivery.deliver")
    deliver_logger.addFilter(stamp)
    try:
        with caplog.at_level(logging.WARNING, logger="django_domain_events.delivery.deliver"):
            deliver_one(row.pk)
    finally:
        deliver_logger.removeFilter(stamp)

    expected = f"{row.pk}:probe.fails"
    assert told == [expected]
    stamped = [
        getattr(r, "current", None) for r in caplog.records if "failed delivery" in r.getMessage()
    ]
    assert stamped == [expected]


def test_an_async_receiver_sees_the_hooks_context_and_its_exception_reaches_exit(
    settings: Any,
) -> None:
    """The hook enters on the calling thread, outside the adapter that runs a
    coroutine; asgiref copies the context into it, and re-raises what the
    coroutine raised, the same object."""
    ran: list[Any] = []
    failure = ValueError("the review platform refused")

    async def asks(event: Unheard) -> None:
        ran.append(CURRENT.get())
        raise failure

    receiver(Unheard, key="probe.async")(asks)
    _hook(settings, "recording")
    row = _owed("probe.async")

    assert deliver_one(row.pk) == DeliveryStatus.FAILED

    assert ran == [f"{row.pk}:probe.async"]
    assert [(exc_type, exc) for exc_type, exc, _ in _exits()] == [(ValueError, failure)]


# Where it does not run


def _orphaned() -> int:
    receiver(Unheard, key="probe.subject")(lambda event: None)
    row = _owed("probe.subject")
    registry._receivers.pop("probe.subject")
    deliver_one(row.pk)
    row.refresh_from_db()
    assert row.status == DeliveryStatus.ORPHANED
    return row.pk


def _undecodable() -> int:
    receiver(Unheard, key="probe.subject")(lambda event: None)
    row = _owed("probe.subject")
    EventRecord.objects.filter(pk=row.event.pk).update(payload={"not_a_field": 1})
    deliver_one(row.pk)
    row.refresh_from_db()
    assert row.status == DeliveryStatus.DEAD
    return row.pk


def _undeclared_event() -> int:
    receiver(Unheard, key="probe.subject")(lambda event: None)
    row = _owed("probe.subject")
    EventRecord.objects.filter(pk=row.event.pk).update(name="nobody.Declared")
    deliver_one(row.pk)
    row.refresh_from_db()
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    return row.pk


def _not_owed() -> int:
    receiver(Unheard, key="probe.subject")(lambda event: None)
    row = _owed("probe.subject")
    DeliveryRecord.objects.filter(pk=row.pk).update(status=DeliveryStatus.SUCCEEDED)
    assert deliver_one(row.pk) is None
    return row.pk


def _lost_before_running() -> int:
    receiver(Unheard, key="probe.subject")(lambda event: None)
    row = _owed("probe.subject")
    claim_batch(worker_id="w1", now=timezone.now(), lease=timedelta(minutes=5), limit=1)
    assert deliver_one(row.pk, worker_id="w2") is None
    return row.pk


def _take_refused() -> int:
    receiver(Unheard, key="probe.subject")(lambda event: None)
    row = _owed("probe.subject")
    assert deliver_one(row.pk, claimed_by="nobody", claimed_at=timezone.now().isoformat()) is None
    return row.pk


@pytest.mark.parametrize(
    "subject",
    [_orphaned, _undecodable, _undeclared_event, _not_owed, _lost_before_running, _take_refused],
    ids=["orphaned", "undecodable", "undeclared-event", "not-owed", "lost", "take-refused"],
)
def test_a_row_that_never_reaches_its_receiver_is_not_wrapped(
    settings: Any, subject: Callable[[], int]
) -> None:
    """No ``DeliveryContext`` exists for these, and each is logged already.
    The control row is delivered afterwards, so the test fails against a tree
    with no hook as well as against one that wraps too early."""
    _hook(settings, "recording")
    subject_pk = subject()

    receiver(Unheard, key="probe.control")(lambda event: None)
    control = _owed("probe.control")
    deliver_one(control.pk)

    assert [context.delivery_id for context in _entered()] == [control.pk]
    assert subject_pk != control.pk


@pytest.mark.parametrize("mode", [DeliveryMode.INLINE, DeliveryMode.ON_COMMIT])
def test_inline_and_on_commit_receivers_are_not_wrapped(
    settings: Any, mode: DeliveryMode, django_capture_on_commit_callbacks: Callable[..., Any]
) -> None:
    """They run in the caller's own request, under whatever context it already
    has. The durable control is delivered in the same test, for the reason the
    test above gives."""
    ran: list[Any] = []
    receiver(Unheard, mode=mode, key="probe.immediate")(_record(ran))
    receiver(Unheard, key="probe.control")(lambda event: None)
    _hook(settings, "recording")

    with django_capture_on_commit_callbacks(execute=True), transaction.atomic():
        event_id = fire(Unheard(value=1))

    assert ran == [None]
    assert event_id is not None
    control = _row(event_id, "probe.control")
    deliver_one(control.pk)
    assert [context.delivery_id for context in _entered()] == [control.pk]


# receiver_key on the context, wherever a receiver is known


def test_a_durable_receiver_is_told_its_own_key(settings: Any) -> None:
    seen: list[Any] = []

    def reads(event: Unheard, context: DeliveryContext) -> None:
        seen.append(getattr(context, "receiver_key", None))

    receiver(Unheard, key="probe.reads", takes_context=True)(reads)
    _hook(settings, None)
    deliver_one(_owed("probe.reads").pk)

    assert seen == ["probe.reads"]


@pytest.mark.parametrize("mode", [DeliveryMode.INLINE, DeliveryMode.ON_COMMIT])
def test_an_immediate_receiver_is_told_its_own_key(
    mode: DeliveryMode, django_capture_on_commit_callbacks: Callable[..., Any]
) -> None:
    """Two receivers of one event, so a context shared between them, or one
    carrying the first receiver's key, is caught."""
    seen: list[tuple[str, Any]] = []

    def first(event: Unheard, context: DeliveryContext) -> None:
        seen.append(("first", getattr(context, "receiver_key", None)))

    def second(event: Unheard, context: DeliveryContext) -> None:
        seen.append(("second", getattr(context, "receiver_key", None)))

    receiver(Unheard, mode=mode, key="probe.first", takes_context=True)(first)
    receiver(Unheard, mode=mode, key="probe.second", takes_context=True)(second)

    with django_capture_on_commit_callbacks(execute=True), transaction.atomic():
        fire(Unheard(value=1))

    assert sorted(seen) == [("first", "probe.first"), ("second", "probe.second")]


def test_a_targets_callable_is_told_the_key_of_the_receiver_it_fans_out() -> None:
    seen: list[Any] = []

    def targets(event: Unheard, context: DeliveryContext) -> list[str]:
        seen.append(getattr(context, "receiver_key", None))
        return ["a"]

    receiver(Unheard, key="probe.fans_out", targets=targets)(lambda event: None)
    receiver(Unheard, key="probe.also_fans_out", targets=targets)(lambda event: None)
    _fire()

    assert sorted(map(str, seen)) == ["probe.also_fans_out", "probe.fans_out"]


def test_a_context_built_by_keyword_without_the_key_still_constructs() -> None:
    """The shape django-outbound-webhooks' tests build one in: every required
    field by keyword, and ``target`` among the defaulted ones. A new field
    added without a default, or ahead of these, breaks every such caller."""
    context = DeliveryContext(
        event_id=1,
        event_name="testapp.OrderPlaced",
        attempt=1,
        actor_key="",
        actor_label="",
        scope={},
        target="endpoint:1",
    )

    assert getattr(context, "receiver_key", None) == ""
    assert (context.target, context.delivery_id, context.due_at) == ("endpoint:1", None, None)
