from __future__ import annotations

import contextlib
import contextvars
import queue
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
from django.db import OperationalError, connection, connections, transaction
from django.utils import timezone

from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery import lane_workers as lane_workers_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.lane_workers import LaneWorkers, Result
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.lane_pause_record import LanePauseRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import event_registered, receiver_registered

pytestmark = pytest.mark.django_db(transaction=True)

Dispatch = Callable[..., DeliveryStatus | None]
SEEN: contextvars.ContextVar[str] = contextvars.ContextVar("lane_workers_seen", default="")


class _Boom(BaseException):
    """A BaseException, which a worker must not swallow."""


@dataclass(frozen=True, slots=True)
class Mail:
    value: int


@pytest.fixture
def stub(monkeypatch: pytest.MonkeyPatch) -> Callable[[Dispatch], None]:
    """Replace ``dispatch_one`` as the workers see it."""
    return lambda fn: monkeypatch.setattr(lane_workers_module, "dispatch_one", fn)


@pytest.fixture
def started() -> Iterator[Callable[[int], LaneWorkers]]:
    """Start workers on the current context, and stop any a test left running."""
    made: list[LaneWorkers] = []

    def start(n: int) -> LaneWorkers:
        made.append(LaneWorkers("mail", n, "relay-1", contextvars.copy_context()))
        return made[-1]

    yield start
    for workers in made:
        with contextlib.suppress(_Boom):
            workers.stop(time.monotonic() + 5)


def _collect(workers: LaneWorkers, count: int, timeout: float = 10) -> list[Result]:
    """Poll the non-blocking ``results`` until ``count`` arrive."""
    deadline = time.monotonic() + timeout
    got: list[Result] = []
    while len(got) < count and time.monotonic() < deadline:
        got.extend(workers.results())
        time.sleep(0.005)
    return got


def test_six_slow_rows_on_six_workers_take_about_one_row(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    def slow(delivery_id: int, **_: Any) -> DeliveryStatus:
        time.sleep(0.25)
        return DeliveryStatus.SUCCEEDED

    stub(slow)
    workers = started(6)
    began = time.monotonic()
    for delivery_id in range(1, 7):
        workers.give(delivery_id)
    got = _collect(workers, 6)
    elapsed = time.monotonic() - began

    assert sorted(got) == [(i, DeliveryStatus.SUCCEEDED, None) for i in range(1, 7)]
    assert elapsed < 0.45, f"six 250 ms rows took {elapsed:.3f} s, so they did not overlap"
    assert all(thread.daemon for thread in workers._threads), "a worker thread is not a daemon"


def test_dispatch_gets_the_worker_id_and_a_deferral_hook(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    def deferring(delivery_id: int, *, worker_id: str, on_deferral: Any) -> None:
        assert worker_id == "relay-1"
        on_deferral("mail", 30.0)

    stub(deferring)
    workers = started(1)
    workers.give(5)

    assert _collect(workers, 1) == [(5, None, ("mail", 30.0))]


def test_idle_slots_count_rows_given_and_not_finished(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    release = threading.Event()
    stub(lambda delivery_id, **_: release.wait(10) and None)
    workers = started(3)
    assert workers.idle_slots() == 3

    workers.give(1)
    workers.give(2)
    assert workers.idle_slots() == 1
    release.set()
    _collect(workers, 2)
    assert workers.idle_slots() == 3


class _SlowPut(queue.SimpleQueue[Result]):
    """A done queue whose ``put`` lingers after the row is visible."""

    def put(self, item: Result, block: bool = True, timeout: float | None = None) -> None:
        super().put(item)
        time.sleep(0.05)


def test_a_reported_row_is_never_counted_busy(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    stub(lambda delivery_id, **_: DeliveryStatus.SUCCEEDED)
    workers = started(2)
    workers._done = _SlowPut()
    workers.give(1)

    assert workers.results(timeout=5) == [(1, DeliveryStatus.SUCCEEDED, None)]
    assert workers.idle_slots() == 2, "results returned a row whose worker still counted busy"


def test_one_results_call_returns_every_row_already_done(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    stub(lambda delivery_id, **_: DeliveryStatus.SUCCEEDED)
    workers = started(2)
    workers.give(1)
    workers.give(2)
    while workers._done.qsize() < 2:
        time.sleep(0.005)

    assert sorted(workers.results()) == [
        (1, DeliveryStatus.SUCCEEDED, None),
        (2, DeliveryStatus.SUCCEEDED, None),
    ]


def test_stop_spends_one_deadline_across_every_worker(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    release = threading.Event()
    entered: queue.SimpleQueue[int] = queue.SimpleQueue()

    def blocked(delivery_id: int, **_: Any) -> None:
        entered.put(delivery_id)
        release.wait(10)

    stub(blocked)
    workers = started(4)
    for delivery_id in range(1, 5):
        workers.give(delivery_id)
    for _ in range(4):
        entered.get(timeout=5)
    began = time.monotonic()
    try:
        workers.stop(began + 0.2)
        elapsed = time.monotonic() - began
    finally:
        release.set()

    assert elapsed < 0.5, f"stop took {elapsed:.3f} s past a 0.2 s deadline over four workers"


def test_a_full_lane_refuses_another_row(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    release = threading.Event()
    stub(lambda delivery_id, **_: release.wait(10) and None)
    workers = started(1)
    workers.give(1)

    with pytest.raises(RuntimeError, match="no idle worker"):
        workers.give(2)
    release.set()


def test_a_context_var_set_by_one_row_is_unseen_by_the_next(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    seen: list[str] = []

    def leaky(delivery_id: int, **_: Any) -> None:
        seen.append(SEEN.get())
        SEEN.set(f"row {delivery_id}")

    stub(leaky)
    workers = started(1)
    workers.give(1)
    _collect(workers, 1)
    workers.give(2)
    _collect(workers, 1)

    assert seen == ["", ""], "a row saw a ContextVar the row before it set"


def test_a_context_var_set_before_start_reaches_every_row(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    seen: list[str] = []
    stub(lambda delivery_id, **_: seen.append(SEEN.get()))
    token = SEEN.set("from startup")
    try:
        workers = started(3)
    finally:
        SEEN.reset(token)
    for delivery_id in range(1, 7):
        _collect(workers, 0, timeout=0)
        while workers.idle_slots() == 0:
            time.sleep(0.005)
        workers.give(delivery_id)
    _collect(workers, 6)

    assert seen == ["from startup"] * 6


def test_a_worker_survives_a_receiver_exception(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    closed: list[int] = []

    def flaky(delivery_id: int, **_: Any) -> DeliveryStatus:
        if delivery_id == 1:
            own = connections[write_alias()]
            own.close = lambda: closed.append(delivery_id)
            raise ValueError("not a database error")
        return DeliveryStatus.SUCCEEDED

    stub(flaky)
    workers = started(1)
    workers.give(1)
    assert _collect(workers, 1) == [(1, None, None)]
    workers.give(2)

    assert _collect(workers, 1) == [(2, DeliveryStatus.SUCCEEDED, None)]
    assert not workers.failed
    assert closed == [], "a receiver error that is not the database closed the connection"


@pytest.mark.parametrize(
    "make_error",
    [
        pytest.param(lambda: OperationalError("the server went away"), id="wrapped"),
        pytest.param(
            lambda: connection.Database.OperationalError("the server went away"), id="raw_driver"
        ),
    ],
)
def test_db_error_closes_own_connection(
    stub: Callable[[Dispatch], None],
    started: Callable[[int], LaneWorkers],
    monkeypatch: pytest.MonkeyPatch,
    make_error: Callable[[], Exception],
) -> None:
    main = connections[write_alias()]
    main.ensure_connection()
    closed: list[Any] = []
    monkeypatch.setattr(main, "close", lambda: closed.append(main))
    worker_side: list[Any] = []

    def broken(delivery_id: int, **_: Any) -> None:
        own = connections[write_alias()]
        own.ensure_connection()
        real_close = own.close
        own.close = lambda: (closed.append(own), real_close())
        worker_side.append(own)
        raise make_error()

    stub(broken)
    workers = started(1)
    workers.give(1)

    assert _collect(workers, 1) == [(1, None, None)]
    (own,) = worker_side
    assert own is not main
    assert closed[:1] == [own], "the worker did not close its own connection"
    assert main not in closed, "a worker closed the main thread's connection"
    assert main.is_usable()
    if connection.vendor != "sqlite":
        assert own.connection is None, "the worker kept its broken connection"


def test_a_failed_close_does_not_end_the_worker(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    def broken(delivery_id: int, **_: Any) -> DeliveryStatus:
        if delivery_id == 1:
            own = connections[write_alias()]
            real_close = own.close

            def refuse() -> None:
                own.close = real_close
                raise OperationalError("could not close")

            own.close = refuse
            raise OperationalError("the server went away")
        return DeliveryStatus.SUCCEEDED

    stub(broken)
    workers = started(1)
    workers.give(1)
    assert _collect(workers, 1) == [(1, None, None)]
    workers.give(2)

    assert _collect(workers, 1) == [(2, DeliveryStatus.SUCCEEDED, None)]
    assert not workers.failed


def test_a_workers_base_exception_reraises_from_stop(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    def fatal(delivery_id: int, **_: Any) -> None:
        raise _Boom("receiver called sys.exit")

    stub(fatal)
    workers = started(2)
    workers.give(1)

    assert _collect(workers, 1) == [(1, None, None)]
    assert workers.failed
    assert workers.idle_slots() == 0
    with pytest.raises(_Boom, match="sys.exit"):
        workers.stop(time.monotonic() + 5)


def test_stop_waits_for_rows_in_flight(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    def slow(delivery_id: int, **_: Any) -> DeliveryStatus:
        time.sleep(0.2)
        return DeliveryStatus.SUCCEEDED

    stub(slow)
    workers = started(2)
    workers.give(1)
    workers.stop(time.monotonic() + 5)

    assert workers.results() == [(1, DeliveryStatus.SUCCEEDED, None)]
    with pytest.raises(RuntimeError, match="stopped"):
        workers.give(2)
    assert workers.idle_slots() == 0


def test_stop_leaves_a_row_running_past_the_deadline_to_its_lease(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    release = threading.Event()
    stub(lambda delivery_id, **_: release.wait(10) and None)
    workers = started(1)
    workers.give(1)
    began = time.monotonic()
    workers.stop(began + 0.1)

    assert time.monotonic() - began < 2
    assert workers.results() == []
    release.set()
    assert _collect(workers, 1) == [(1, None, None)]


@pytest.fixture
def mail_lane(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """A mail-lane receiver that always defers an hour without counting."""

    def mail(evt: Mail) -> None:
        raise RetryAfter(3600, reason="quota", counts=False)

    receiver = RegisteredReceiver(
        key="tests.lane_workers_mail",
        event_class=Mail,
        func=mail,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane="mail",
        give_up_after=timedelta(days=1),
    )
    monkeypatch.setattr(deliver_module.random, "random", lambda: 0.5)
    with event_registered(Mail, "tests.LaneWorkersMail"), receiver_registered(receiver):
        yield


@pytest.mark.usefixtures("mail_lane")
def test_a_worker_reports_a_retry_after_and_never_writes_the_pause(
    started: Callable[[int], LaneWorkers],
) -> None:
    with transaction.atomic():
        fire(Mail(value=1))
    (delivery_id,) = claim_batch(
        worker_id="relay-1", now=timezone.now(), lease=timedelta(seconds=300), limit=10
    )
    workers = started(1)
    workers.give(delivery_id)

    ((got_id, _, deferral),) = _collect(workers, 1)
    assert (got_id, deferral) == (delivery_id, ("mail", 3600.0))
    assert not LanePauseRecord.objects.exists(), "a worker wrote the lane pause"


def _backends() -> int:
    """Other client connections to this database, as Postgres sees them."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
            "AND pid <> pg_backend_pid() AND backend_type = 'client backend'"
        )
        return cursor.fetchone()[0]


def test_no_connection_is_left_after_stop(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    if connection.vendor != "postgresql":
        pytest.skip("pg_stat_activity is Postgres")
    before = _backends()

    def querying(delivery_id: int, **_: Any) -> DeliveryStatus:
        DeliveryRecord.objects.exists()
        return DeliveryStatus.SUCCEEDED

    stub(querying)
    workers = started(3)
    for delivery_id in range(1, 4):
        workers.give(delivery_id)
    _collect(workers, 3)
    assert _backends() > before, "the workers opened no connection, so this proves nothing"
    workers.stop(time.monotonic() + 5)

    deadline = time.monotonic() + 3
    while _backends() > before and time.monotonic() < deadline:
        time.sleep(0.02)
    assert _backends() == before, "a worker's connection outlived stop"


def test_no_connection_is_left_after_a_worker_raises_a_base_exception(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    if connection.vendor != "postgresql":
        pytest.skip("pg_stat_activity is Postgres")
    pids: list[int] = []

    def querying_then_fatal(delivery_id: int, **_: Any) -> None:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_backend_pid()")
            pids.append(cursor.fetchone()[0])
        raise _Boom("receiver called sys.exit")

    stub(querying_then_fatal)
    workers = started(1)
    workers.give(1)
    assert _collect(workers, 1) == [(1, None, None)]
    with pytest.raises(_Boom):
        workers.stop(time.monotonic() + 5)

    def alive() -> int:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE pid = %s", pids)
            return cursor.fetchone()[0]

    deadline = time.monotonic() + 3
    while alive() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not alive(), "a worker's connection outlived its BaseException"


def test_results_waits_up_to_its_timeout_for_the_first_row(
    stub: Callable[[Dispatch], None], started: Callable[[int], LaneWorkers]
) -> None:
    def slow(delivery_id: int, **_: Any) -> DeliveryStatus:
        time.sleep(0.2 if delivery_id == 1 else 0)
        return DeliveryStatus.SUCCEEDED

    stub(slow)
    workers = started(2)
    began = time.monotonic()
    assert workers.results(timeout=0.05) == []
    assert time.monotonic() - began >= 0.04, "an empty wait returned before its timeout"

    workers.give(1)
    got = workers.results(timeout=5)
    waited = time.monotonic() - began
    assert got == [(1, DeliveryStatus.SUCCEEDED, None)]
    assert 0.15 < waited < 2, f"results waited {waited:.3f} s for a 200 ms row"
