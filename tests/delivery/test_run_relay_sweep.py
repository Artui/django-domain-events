from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import OperationalError, connection, connections, transaction
from django.utils import timezone as django_timezone

from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.models.receiver_last_success import ReceiverLastSuccess
from django_domain_events.operations.prune_events import prune_events
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.pruned import Pruned
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered, receiver_replaced
from tests.testapp.events import OrderPlaced, PinnedName, Unheard, calls

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
    """An event the prune deletes at its first sweep: consumed with no delivery rows."""
    return EventRecord.objects.create(
        name="testapp.OrderPlaced",
        payload={},
        occurred_at=django_timezone.now(),
        delete_when="settled",
    )


def test_an_idle_relay_deletes_what_has_been_consumed(settings) -> None:
    """End to end, with no spy: a consumed event is gone after the idle interval."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 0.001}
    _consumed_event()

    run_relay(worker_id="w1", passes=3, wait=lambda _: bool(time.sleep(0.01)), **UNSAFE)

    assert not EventRecord.objects.exists()


class _Clock:
    """The sweep's clock, which advances only when the relay waits."""

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
    """The prune the relay calls, replaced by a record of when it was called."""

    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.at: list[float] = []
        self.raises: list[Exception] = []

    def __call__(self, *, stop: Callable[[], bool] | None = None) -> Pruned:
        self.at.append(self.clock.t)
        if self.raises:
            raise self.raises.pop(0)
        return Pruned(0, 0)


def _relay(
    monkeypatch: pytest.MonkeyPatch, clock: _Clock, passes: int, **kwargs: object
) -> _Sweeps:
    """Run ``passes`` passes on ``clock`` with the prune replaced."""
    sweeps = _Sweeps(clock)
    monkeypatch.setattr(run_relay_module, "prune_with_counts", sweeps)
    run_relay(worker_id="w1", passes=passes, monotonic=clock, wait=clock.wait, **UNSAFE, **kwargs)
    return sweeps


def test_an_idle_relay_sweeps_once_per_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Five idle passes at 0, 30, 60, 90 and 120 seconds, against 60."""
    sweeps = _relay(monkeypatch, _Clock(30, 30, 30, 30), passes=5)

    assert sweeps.at == [60.0, 120.0]


def test_the_first_sweep_is_an_interval_after_the_relay_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A relay restarted more often than the interval never sweeps."""
    sweeps = _relay(monkeypatch, _Clock(), passes=3)

    assert sweeps.at == []


def _late_relay(monkeypatch: pytest.MonkeyPatch, passes: int = 1, **kwargs: object) -> _Sweeps:
    """Clock reads zero, then 1000 s, so every idle pass is past the interval."""
    clock = _Clock()
    sweeps = _Sweeps(clock)
    monkeypatch.setattr(run_relay_module, "prune_with_counts", sweeps)
    readings = iter([0.0])
    run_relay(
        worker_id="w1",
        passes=passes,
        monotonic=lambda: next(readings, 1000.0),
        wait=clock.wait,
        **UNSAFE,
        **kwargs,
    )
    return sweeps


def test_a_pass_that_claimed_work_does_not_sweep(
    monkeypatch: pytest.MonkeyPatch, order: OrderPlaced, record: list[str]
) -> None:
    """A relay with work does not stop to prune: the sweep waits for an idle pass."""
    with transaction.atomic():
        fire(order)

    assert _late_relay(monkeypatch).at == []
    assert len(_late_relay(monkeypatch).at) == 1


def test_a_relay_with_the_sweep_turned_off_never_prunes(
    monkeypatch: pytest.MonkeyPatch, settings
) -> None:
    """For a project scheduling prune_events itself, sweep is skipped."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE": False}

    assert _late_relay(monkeypatch).at == []


def test_the_interval_is_the_setting(monkeypatch: pytest.MonkeyPatch, settings) -> None:
    """Passes at 0, 5 and 10 s, interval 10: the setting is read, not a constant."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 10}

    assert _relay(monkeypatch, _Clock(5, 5), passes=3).at == [10.0]


def test_a_named_lane_sweeps_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any relay sweeps, so lane-only deployments still prune."""
    with receiver_registered(_mail()):
        sweeps = _late_relay(monkeypatch, lane="mail")

    assert len(sweeps.at) == 1


def _lane(lane: str, event_class: type, **options: object) -> RegisteredReceiver:
    """A lane has rows only when a test fires an event no receiver listens to."""
    return RegisteredReceiver(
        key=f"tests.sweep_{lane}",
        event_class=event_class,
        func=lambda evt: calls.append(lane),
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane=lane,
        **options,
    )


def test_a_lane_with_work_keeps_the_pass_from_idling(
    monkeypatch: pytest.MonkeyPatch, record: list[str]
) -> None:
    """push claims work and sms, last, claims nothing: the pass is not idle."""
    with receiver_registered(_lane("push", Unheard)), receiver_registered(_lane("sms", PinnedName)):
        with transaction.atomic():
            fire(Unheard(value=1))
        sweeps = _late_relay(monkeypatch, lane=["push", "sms"])

    assert calls == ["push"]
    assert sweeps.at == []
    assert sweeps.clock.waits == 0


def test_a_pass_whose_every_lane_is_empty_is_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    with receiver_registered(_lane("push", Unheard)), receiver_registered(_lane("sms", PinnedName)):
        sweeps = _late_relay(monkeypatch, lane=["push", "sms"])

    assert len(sweeps.at) == 1
    assert sweeps.clock.waits == 1


def test_a_pass_whose_lanes_are_empty_or_paused_is_idle(
    monkeypatch: pytest.MonkeyPatch, record: list[str]
) -> None:
    """The first pass claims SMS, which defers for an hour."""

    def deferring(evt: PinnedName) -> None:
        calls.append("sms")
        raise RetryAfter(3600, reason="credit", counts=False)

    sms = _lane("sms", PinnedName, give_up_after=timedelta(days=1))
    with receiver_registered(_lane("push", Unheard)), receiver_registered(sms):
        with transaction.atomic():
            fire(PinnedName(value=1))
        with receiver_replaced(sms.key, deferring):
            sweeps = _late_relay(monkeypatch, passes=2, lane=["push", "sms"])

    assert calls == ["sms"]
    assert len(sweeps.at) == 1
    assert sweeps.clock.waits == 1


def test_a_single_pass_never_sweeps(order: OrderPlaced, record: list[str]) -> None:
    """deliver_events --once belongs in a cron that can run prune_events."""
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
    """Passes at 0, 60, 90 and 120: the sweep due at 60 raises, so 90 does not retry."""
    clock = _Clock(60, 30, 30)
    sweeps = _Sweeps(clock)
    sweeps.raises.append(error)
    monkeypatch.setattr(run_relay_module, "prune_with_counts", sweeps)
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
    """Consumed events with four successes each: every batch moves last-success."""
    base = django_timezone.now() - timedelta(hours=1)
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
    """One relay's sweep, on its own connection, in step with the other."""
    try:
        connection.ensure_connection()
        barrier.wait(timeout=10)
        results.append(prune_events(batch_size=size))
    except BaseException as exc:
        errors.append(exc)
    finally:
        connections.close_all()


_NEEDS_A_LOCKING_SERVER = pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="two sweeps contending for the same rows needs a server that locks them",
)


def _race(sizes: tuple[int, int]) -> tuple[datetime, list[int], list[BaseException]]:
    """Two sweeps over the same events at once, each with its own batch size."""
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
    """The tables are empty and every receiver's success is the newest."""
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
    """Relays may sweep together: the delete re-checks, so a repeat removes nothing."""
    newest, results, errors = _race(sizes)

    assert errors == []
    assert len(results) == 2, "a sweeping thread did not finish"
    _assert_everything_went_once(newest, sum(results))


@_NEEDS_A_LOCKING_SERVER
@pytest.mark.parametrize(
    "sizes", [(5, 13), (13, 5), (9, 50), (500, 7), (3, 5)], ids=lambda s: "x".join(map(str, s))
)
def test_two_relays_with_different_batches_never_deadlock(
    sizes: tuple[int, int],
) -> None:
    """Opposite-order last-success writes deadlock about half the time on 9x50."""
    newest, results, errors = _race(sizes)

    assert errors == []
    assert len(results) == 2, "a sweeping thread did not finish"
    _assert_everything_went_once(newest, sum(results))


def test_a_stop_request_waits_for_one_batch_of_the_sweep_not_all_of_it(settings) -> None:
    """The relay hands its ``stop`` to the prune, which reads it between batches."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 0.001, "PRUNE_BATCH_ROWS": 1}
    for _ in range(5):
        _consumed_event()

    run_relay(
        worker_id="w1",
        passes=5,
        stop=lambda: EventRecord.objects.count() < 5,
        wait=lambda _: bool(time.sleep(0.01)),
        **UNSAFE,
    )

    assert EventRecord.objects.count() == 4


def test_a_sweep_logs_one_line_with_events_delivery_rows_and_duration(
    settings, caplog: pytest.LogCaptureFixture
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 0.001}
    for _ in range(2):
        event = _consumed_event()
        for key in ("a", "b", "c"):
            DeliveryRecord.objects.create(
                event=event,
                receiver_key=f"tests.{key}",
                status=DeliveryStatus.SUCCEEDED,
                available_at=django_timezone.now(),
                succeeded_at=django_timezone.now(),
            )

    with caplog.at_level(logging.INFO, logger=run_relay_module.logger.name):
        run_relay(worker_id="w1", passes=3, wait=lambda _: bool(time.sleep(0.01)), **UNSAFE)

    lines = [r.getMessage() for r in caplog.records if "pruned" in r.getMessage()]
    assert len(lines) == 1
    assert re.fullmatch(r"relay w1 pruned 2 events and 6 delivery rows in \d+\.\d{3} s", lines[0])


def test_prune_events_still_returns_the_event_count() -> None:
    event = _consumed_event()
    DeliveryRecord.objects.create(
        event=event,
        receiver_key="tests.a",
        status=DeliveryStatus.SUCCEEDED,
        available_at=django_timezone.now(),
        succeeded_at=django_timezone.now(),
    )

    assert prune_events() == 1


def test_a_sweep_that_deleted_only_delivery_rows_logs_its_line(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A stop mid-chunk deletes delivery rows and no event; the sweep still says so."""
    monkeypatch.setattr(run_relay_module, "prune_with_counts", lambda **_: Pruned(0, 3))
    with caplog.at_level(logging.INFO, logger=run_relay_module.logger.name):
        run_relay_module._sweep_or_survive("w1", None, lambda: False)
    lines = [r.getMessage() for r in caplog.records if "pruned" in r.getMessage()]
    assert len(lines) == 1
    assert lines[0].startswith("relay w1 pruned 0 events and 3 delivery rows in ")
