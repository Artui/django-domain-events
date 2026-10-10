"""The relay's idle prune sweep (``run_relay``'s ``RELAY_PRUNE`` path).

A file of its own beside ``test_run_relay.py``: the sweep is a throttle and a
setting pair around one call, and its tests share fixtures with nothing in the
claim and delivery tests.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import OperationalError, connection, connections, transaction

from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.models.receiver_last_success import ReceiverLastSuccess
from django_domain_events.operations.prune_events import prune_events
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered
from tests.testapp.events import OrderPlaced, calls

pytestmark = pytest.mark.django_db(transaction=True)

UNSAFE = {"allow_unsafe_concurrency": True}


def _mail() -> RegisteredReceiver:
    """A receiver in a named lane, so a relay can serve that lane."""
    return RegisteredReceiver(
        key="tests.sweep_mail",
        event_class=OrderPlaced,
        func=lambda evt: calls.append("mail"),
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane="mail",
    )


def _consumed_event() -> EventRecord:
    """An event the prune deletes at its first sweep: it asks to go once
    consumed and has no delivery rows, so it is consumed as it commits. Built
    directly, because the prune reads only the row and never the registry."""
    return EventRecord.objects.create(
        name="testapp.OrderPlaced",
        payload={},
        occurred_at=datetime.now(timezone.utc),
        delete_when="settled",
    )


def test_an_idle_relay_deletes_what_has_been_consumed(settings) -> None:
    """End to end, with nothing spying on the sweep: a consumed event is gone
    after the relay idles past the interval, with no cron line anywhere."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 0.001}
    _consumed_event()

    run_relay(worker_id="w1", passes=3, wait=lambda _: bool(time.sleep(0.01)), **UNSAFE)

    assert not EventRecord.objects.exists()


class _Clock:
    """The sweep's clock, which advances only when the relay waits.

    Each idle pass waits once, so the n-th idle pass reads the sum of the first
    n-1 steps. A pass that claimed work does not wait and the clock stands
    still, which is what lets a test place a pass at an exact moment.
    """

    def __init__(self, *steps: float, start: float = 0.0) -> None:
        self.t = start
        self.steps = list(steps)
        self.waits = 0

    def __call__(self) -> float:
        return self.t

    def wait(self, timeout: float) -> bool:
        self.waits += 1
        self.t += self.steps.pop(0) if self.steps else 0.0
        return False


class _Sweeps:
    """The prune the relay calls, replaced by a record of when it was called.

    Reads the clock of the run it is called in, so each entry is the moment a
    sweep ran. The real prune is out of the picture: these tests ask only when
    the relay sweeps, and the end-to-end ones above ask whether it works.
    """

    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.at: list[float] = []
        self.raises: list[Exception] = []

    def __call__(self) -> int:
        self.at.append(self.clock.t)
        if self.raises:
            raise self.raises.pop(0)
        return 0


def _relay(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock, passes: int, **kwargs: object
) -> _Sweeps:
    """Run ``passes`` passes on ``clock`` with the prune replaced."""
    sweeps = _Sweeps(clock)
    monkeypatch.setattr(run_relay_module, "prune_events", sweeps)
    run_relay(worker_id="w1", passes=passes, monotonic=clock, wait=clock.wait, **UNSAFE, **kwargs)
    return sweeps


def test_an_idle_relay_sweeps_once_per_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Five idle passes at 0, 30, 60, 90 and 120 seconds, against 60. The 60 is
    exactly one interval after the start, which holds ``>=`` against ``>``; the
    30 and the 90 are inside an interval, which holds the throttle; and the 120
    is a full interval after the 60 rather than after the 90, which holds that
    only a sweep moves the clock (an idle pass that moved it would put the 60's
    successor at 150)."""
    sweeps = _relay(monkeypatch, _Clock(30, 30, 30, 30), passes=5)

    assert sweeps.at == [60.0, 120.0]


def test_the_first_sweep_is_an_interval_after_the_relay_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Not at the first idle pass: a relay run for a few passes, or restarted
    more often than the interval, then never sweeps."""
    sweeps = _relay(monkeypatch, _Clock(), passes=3)

    assert sweeps.at == []


def _late_relay(monkeypatch: pytest.MonkeyPatch, **kwargs: object) -> _Sweeps:
    """One pass of a relay whose clock reads zero at its start and 1000 seconds
    on every reading after, so any idle pass is long past the interval."""
    clock = _Clock()
    sweeps = _Sweeps(clock)
    monkeypatch.setattr(run_relay_module, "prune_events", sweeps)
    readings = iter([0.0])
    run_relay(
        worker_id="w1",
        passes=1,
        monotonic=lambda: next(readings, 1000.0),
        wait=clock.wait,
        **UNSAFE,
        **kwargs,
    )
    return sweeps


def test_a_pass_that_claimed_work_does_not_sweep(
    monkeypatch: pytest.MonkeyPatch, order: OrderPlaced, record: list[str]
) -> None:
    """A relay with work does not stop to prune: the sweep waits for a pass that
    claimed nothing, however overdue it is. The second run is the control, the
    same clock on the pass the work has left (``if not ids`` is what fails
    here)."""
    with transaction.atomic():
        fire(order)

    assert _late_relay(monkeypatch).at == []
    assert len(_late_relay(monkeypatch).at) == 1


def test_a_relay_with_the_sweep_turned_off_never_prunes(
    monkeypatch: pytest.MonkeyPatch, settings
) -> None:
    """For a project that schedules ``prune_events`` itself (``sweep`` in the
    idle branch's guard)."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE": False}

    assert _late_relay(monkeypatch).at == []


def test_the_interval_is_the_setting(monkeypatch: pytest.MonkeyPatch, settings) -> None:
    """Passes at 0, 5 and 10 seconds against an interval of 10: the interval
    read is ``RELAY_PRUNE_SECONDS`` and not a constant."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 10}

    assert _relay(monkeypatch, _Clock(5, 5), passes=3).at == [10.0]


def test_a_named_lane_sweeps_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any relay sweeps: the interval is what keeps several cheap, and a
    deployment running only lane relays would otherwise never prune."""
    with receiver_registered(_mail()):
        sweeps = _late_relay(monkeypatch, lane="mail")

    assert len(sweeps.at) == 1


def test_a_single_pass_never_sweeps(order: OrderPlaced, record: list[str]) -> None:
    """``deliver_events --once`` is a cron's job, run from a schedule that can
    carry ``prune_events`` itself. Real rows rather than a spy, so it fails if
    the single pass ever grows a sweep by any route."""
    _consumed_event()

    call_command("deliver_events", "--once", stdout=StringIO())

    assert EventRecord.objects.count() == 1


@pytest.mark.parametrize(
    ("error", "closed"),
    [(OperationalError("the connection is lost"), [write_alias()]), (RuntimeError("bug"), [])],
    ids=["database", "other"],
)
def test_a_sweep_that_fails_does_not_kill_the_relay_or_retry_early(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    closed: list[str],
) -> None:
    """Passes at 0, 60, 90 and 120: the sweep due at 60 raises, the 90 is inside
    the interval that attempt started and so does not try again, and the 120
    does. The relay finishes all four passes. A database error closes the
    connection, as a failed claim does; anything else leaves it alone."""
    clock = _Clock(60, 30, 30)
    sweeps = _Sweeps(clock)
    sweeps.raises.append(error)
    monkeypatch.setattr(run_relay_module, "prune_events", sweeps)
    closes: list[str] = []
    conn = run_relay_module.connections[write_alias()]
    real_close = conn.close
    monkeypatch.setattr(conn, "close", lambda: (closes.append(conn.alias), real_close())[1])

    with caplog.at_level(logging.INFO, logger=run_relay_module.logger.name):
        run_relay(worker_id="w1", passes=4, monotonic=clock, wait=clock.wait, **UNSAFE)

    assert sweeps.at == [60.0, 120.0]
    assert clock.waits == 4, "the relay stopped at the failed sweep"
    assert "could not prune" in caplog.text
    assert closes == closed


def test_a_sweep_reports_what_it_deleted(settings, caplog: pytest.LogCaptureFixture) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 0.001}
    _consumed_event()

    with caplog.at_level(logging.INFO, logger=run_relay_module.logger.name):
        run_relay(worker_id="w1", passes=3, wait=lambda _: bool(time.sleep(0.01)), **UNSAFE)

    assert "pruned 1 events" in caplog.text


# --- several relays, several sweeps ---------------------------------------------

_RECEIVERS = ("tests.r0", "tests.r1", "tests.r2", "tests.r3")
_EVENTS = 200


def _fan_out_events(count: int) -> datetime:
    """``count`` consumed events of four succeeded deliveries each, the nth
    having succeeded n seconds after the base, so every batch moves every
    receiver's recorded last success forward - the contended write. Returns the
    newest success."""
    base = datetime.now(timezone.utc) - timedelta(hours=1)
    for n in range(count):
        event = EventRecord.objects.create(
            name="testapp.OrderPlaced", payload={}, occurred_at=base, delete_when="succeeded"
        )
        succeeded_at = base + timedelta(seconds=n)
        DeliveryRecord.objects.bulk_create(
            DeliveryRecord(
                event=event,
                receiver_key=key,
                status=DeliveryStatus.SUCCEEDED,
                available_at=base,
                succeeded_at=succeeded_at,
            )
            for key in _RECEIVERS
        )
    return base + timedelta(seconds=count - 1)


def _sweep(
    barrier: threading.Barrier, size: int, results: list[int], errors: list[BaseException]
) -> None:
    """One relay's sweep, on this thread's own connection and in step with the
    other, so both select the same due events and both delete them."""
    try:
        # Connected before the barrier, or the second thread spends the first
        # one's whole sweep opening its connection and nothing overlaps.
        connection.ensure_connection()
        barrier.wait(timeout=10)
        results.append(prune_events(batch_size=size))
    except BaseException as exc:
        errors.append(exc)
    finally:
        connections.close_all()


_NEEDS_A_LOCKING_SERVER = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="two sweeps contending for the same rows needs a server that locks them",
)


def _race(sizes: tuple[int, int]) -> tuple[datetime, list[int], list[BaseException]]:
    """Two sweeps over the same events at once, each with its own batch size.
    Returns the newest success among the events, what each sweep deleted, and
    what each raised."""
    newest = _fan_out_events(_EVENTS)
    results: list[int] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)
    threads = [
        threading.Thread(target=_sweep, args=(barrier, size, results, errors)) for size in sizes
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    return newest, results, errors


def _assert_everything_went_once(newest: datetime, deleted: int | None) -> None:
    """The tables are empty and every receiver's success is the newest. With a
    count, also that each event was counted once: ``None`` for a run in which a
    sweep died, whose committed batches died uncounted with it."""
    if deleted is not None:
        assert deleted == _EVENTS, f"{deleted}: an event was deleted twice or not at all"
    assert not EventRecord.objects.exists()
    assert not DeliveryRecord.objects.exists()
    recorded = dict(ReceiverLastSuccess.objects.values_list("receiver_key", "last_succeeded_at"))
    assert recorded == {key: newest for key in _RECEIVERS}


@_NEEDS_A_LOCKING_SERVER
@pytest.mark.parametrize(
    "sizes", [(5, 5), (5, 5), (5, 5), (3, 3)], ids=lambda s: "x".join(map(str, s))
)
def test_two_relays_sweeping_the_same_batches_neither_fail_nor_delete_twice(
    sizes: tuple[int, int],
) -> None:
    """The reason N relays may each sweep: the prune re-checks at the delete that
    an event is still due, and a delete of a row another sweep already took
    removes nothing. Real connections, on every event at the same moment.

    Asserts what a second sweep could get wrong - an error, the same event
    counted by both, or a receiver's recorded success left behind - and would
    still pass if the sweeps never overlapped. They do: started together on
    connections already open, each makes up to 200 transactions over the same
    200 events (a batch of five rows is one event; a batch of three is smaller
    than an event, so those sweeps delete in chunks), and the loser of each
    event trails the winner by one. Same-sized batches started together take the
    receivers' last-success rows in the same order, so this lockstep case does
    not deadlock; real relays are not in lockstep, and the test below is the
    realistic one."""
    newest, results, errors = _race(sizes)

    assert errors == []
    assert len(results) == 2, "a sweeping thread did not finish"
    _assert_everything_went_once(newest, sum(results))


@_NEEDS_A_LOCKING_SERVER
@pytest.mark.parametrize(
    "sizes", [(5, 13), (13, 5), (9, 50), (500, 7), (3, 5)], ids=lambda s: "x".join(map(str, s))
)
def test_two_relays_with_different_batches_lose_at_worst_a_sweep_to_a_deadlock(
    sizes: tuple[int, int],
) -> None:
    """Sweeps whose batches differ take the receivers' last-success rows in
    different orders, because ``_record_last_successes`` walks an unordered
    GROUP BY, and two transactions taking the same rows in opposite orders
    deadlock: about one run in two on the ``9x50`` case, on the insert of a
    receiver's first row and on the update of a later one alike. Postgres kills
    one transaction, which the relay survives (``could not prune``, retried an
    interval later), so this asserts the failure is bounded: the only error is a
    detected deadlock, nothing is deleted twice, and one more sweep leaves the
    table as a single sweep would (and where neither sweep died, nothing was
    counted twice).

    Walking the keys in sorted order in both of its loops removes the cause
    (ten runs in a row of this file, with the case above, and none failed); it
    is the prune's to change, and when it does this test's tolerance goes."""
    newest, results, errors = _race(sizes)

    assert all("deadlock detected" in str(e) for e in errors), errors
    assert len(results) + len(errors) == 2, "a sweeping thread did not finish"
    left = prune_events()
    _assert_everything_went_once(newest, None if errors else sum(results) + left)
