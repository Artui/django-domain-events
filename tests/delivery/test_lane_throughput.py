from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta

import pytest
from django.db import connection, transaction

from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.lane_rate import LaneRate
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import event_registered, receiver_registered

pytestmark = pytest.mark.django_db(transaction=True)

STUB_SECONDS = 0.25
ROWS = 36


@dataclass(frozen=True, slots=True)
class Call:
    value: int


@dataclass
class _Stub:
    """A provider that takes ``STUB_SECONDS`` a call, recording when each call began and ended."""

    starts: list[float]
    ends: list[float]
    lock: threading.Lock

    def __call__(self, evt: Call) -> None:
        with self.lock:
            self.starts.append(time.monotonic())
        time.sleep(STUB_SECONDS)
        with self.lock:
            self.ends.append(time.monotonic())


@pytest.fixture
def stub() -> Iterator[_Stub]:
    if connection.vendor != "postgresql":
        pytest.skip("real threads on real connections: the test-postgres job measures this")
    provider = _Stub(starts=[], ends=[], lock=threading.Lock())
    entry = RegisteredReceiver(
        key="tests.provider",
        event_class=Call,
        func=provider,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane="provider",
        give_up_after=timedelta(days=1),
    )
    with event_registered(Call, "tests.Call"), receiver_registered(entry):
        yield provider


def _fire(count: int) -> None:
    with transaction.atomic():
        for value in range(count):
            fire(Call(value=value))


def _report(name: str, line: str) -> None:
    """Append one measured figure to ``DDE_THROUGHPUT_LOG``, for a run repeated by hand."""
    path = os.environ.get("DDE_THROUGHPUT_LOG")
    if path:
        with open(path, "a") as log:
            log.write(f"{name} {line}\n")


def _until(condition: Callable[[], bool], seconds: float = 60.0) -> Callable[[], bool]:
    deadline = time.monotonic() + seconds
    return lambda: condition() or time.monotonic() > deadline


def test_six_workers_deliver_at_least_four_times_faster_than_one(stub: _Stub) -> None:
    """Against the serial floor ROWS x STUB_SECONDS, which a one-worker run can only exceed."""
    _fire(ROWS)

    began = time.monotonic()
    counts = run_relay(
        worker_id="w1",
        lane="provider",
        lane_concurrency={"provider": 6},
        stop=_until(lambda: len(stub.ends) == ROWS),
    )

    elapsed = max(stub.ends) - began
    speedup = ROWS * STUB_SECONDS / elapsed
    _report("speedup", f"{speedup:.2f}")
    assert counts == {DeliveryStatus.SUCCEEDED: ROWS}
    assert speedup >= 4, f"{ROWS} rows took {elapsed:.2f}s at N=6: {speedup:.2f}x"


def test_a_rate_of_five_puts_no_more_than_five_starts_in_any_second(
    stub: _Stub, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A start is the relay handing a row to a worker, the moment its rate gate records."""
    rows = 16
    _fire(rows)
    handed: list[float] = []
    record = LaneRate.started

    def started(gate: LaneRate) -> None:
        handed.append(time.monotonic())
        record(gate)

    monkeypatch.setattr(LaneRate, "started", started)

    counts = run_relay(
        worker_id="w1",
        lane="provider",
        lane_concurrency={"provider": 10},
        lane_rates={"provider": 5},
        stop=_until(lambda: len(stub.ends) == rows),
    )

    closest = _closest_six(handed)
    _report("six-start-window", f"{closest:.4f} provider {_closest_six(stub.starts):.4f}")
    assert counts == {DeliveryStatus.SUCCEEDED: rows}
    assert len(handed) == rows
    assert DeliveryRecord.objects.filter(attempts=1).count() == rows
    assert closest >= 1.0, f"six starts within {closest:.4f}s"


def _closest_six(moments: list[float]) -> float:
    """The shortest span holding six of ``moments``: under one second is six starts in a second."""
    ordered = sorted(moments)
    return min(later - earlier for earlier, later in zip(ordered, ordered[5:], strict=False))
