from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest
from django.db import OperationalError, transaction
from django.utils import timezone as django_timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.lane_rate import LaneRate
from django_domain_events.delivery.lane_workers import LaneWorkers, Result
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.utils import GIVEN_BACK, lane_threads
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.lane_pause_record import LanePauseRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import in_project_time
from tests.conftest import event_registered, receiver_registered, receiver_replaced

pytestmark = pytest.mark.django_db(transaction=True)

UNSAFE = {"allow_unsafe_concurrency": True}

CALLS: list[int] = []
"""The ``value`` of each event the mail receiver was called with, in call order."""


@dataclass(frozen=True, slots=True)
class Push:
    value: int


@dataclass(frozen=True, slots=True)
class Mail:
    value: int


class _Boom(BaseException):
    """Raised by a receiver to end the process, as ``SystemExit`` would."""


def _receiver(lane: str, event_class: type, **options: Any) -> RegisteredReceiver:
    options.setdefault("func", lambda evt: CALLS.append(evt.value))
    return RegisteredReceiver(
        key=f"tests.{lane}",
        event_class=event_class,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane=lane,
        give_up_after=timedelta(days=1),
        **options,
    )


@pytest.fixture
def lanes() -> Iterator[None]:
    """``push`` and ``mail``; the mail receiver records each call in ``CALLS``."""
    CALLS.clear()
    with (
        event_registered(Push, "tests.Push"),
        event_registered(Mail, "tests.Mail"),
        receiver_registered(_receiver("push", Push, lease_seconds=60)),
        receiver_registered(_receiver("mail", Mail, lease_seconds=30)),
    ):
        yield
    CALLS.clear()


def _fire(count: int) -> list[int]:
    with transaction.atomic():
        for value in range(count):
            fire(Mail(value=value))
    return list(DeliveryRecord.objects.order_by("pk").values_list("pk", flat=True))


def _given_back(pk: int) -> bool:
    return DeliveryRecord.objects.get(pk=pk).lease_expires_at == in_project_time(GIVEN_BACK)


def _settled(pk: int) -> bool:
    return DeliveryRecord.objects.get(pk=pk).status == DeliveryStatus.SUCCEEDED


def _untouched(pk: int) -> bool:
    return DeliveryRecord.objects.get(pk=pk).claimed_by == ""


class _Clock:
    """A wall clock and a monotonic one, both moved on only by the relay's sleeps and waits."""

    def __init__(self, step: float = 0.0) -> None:
        self.at = django_timezone.now()
        self.mono = 0.0
        self.step = step
        self.sleeps: list[float] = []
        self.waits = 0

    def __call__(self) -> datetime:
        return self.at

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.at += timedelta(seconds=seconds)
        self.mono += seconds

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)

    def wait(self, timeout: float) -> bool:
        self.waits += 1
        self.advance(self.step)
        return False


class _InlineWorkers(LaneWorkers):
    """LaneWorkers with no threads: a row is delivered on the giving thread, so SQLite runs it.

    With ``prompt`` false a result is reported only to a waiting ``results(timeout)``."""

    prompt = True
    made: list[_InlineWorkers] = []

    def __init__(self, lane: str, n: int, worker_id: str, context: Context) -> None:
        super().__init__(lane, 0, worker_id, context)
        self._n = n
        self.finished: list[Result] = []
        self.deadlines: list[float] = []
        _InlineWorkers.made.append(self)

    def give(self, delivery_id: int) -> None:
        super().give(delivery_id)
        self._handoff.get()
        try:
            result = self._context.copy().run(self._deliver, delivery_id)
        except BaseException as exc:
            self._raised = self._raised or exc
            result = (delivery_id, None, None)
        if self.prompt:
            self._report(result)
        else:
            self.finished.append(result)

    def results(self, timeout: float = 0.0) -> list[Result]:
        if timeout > 0:
            for result in self.finished:
                self._report(result)
            self.finished.clear()
        return super().results()

    def stop(self, deadline: float) -> None:
        self.deadlines.append(deadline)
        for result in self.finished:
            self._report(result)
        self.finished.clear()
        super().stop(deadline)


@pytest.fixture
def inline(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[_InlineWorkers]]:
    """Run the threaded loop on ``_InlineWorkers``, prompt unless the test says otherwise."""
    monkeypatch.setattr(run_relay_module, "LaneWorkers", _InlineWorkers)
    monkeypatch.setattr(_InlineWorkers, "prompt", True)
    monkeypatch.setattr(deliver_module.random, "random", lambda: 0.0)
    _InlineWorkers.made.clear()
    yield _InlineWorkers
    _InlineWorkers.made.clear()


def _claims(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Every claim the relay makes, as the keyword arguments it made it with."""
    made: list[dict[str, Any]] = []
    real = run_relay_module.claim_batch

    def spy(**kwargs: Any) -> list[int]:
        made.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(run_relay_module, "claim_batch", spy)
    return made


def _mail_says(func: Callable[[Mail], None]) -> Any:
    return receiver_replaced("tests.mail", func)


# --- Refusals: one threaded lane per relay, sized by its workers -------------


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"lane": ["push", "mail"], "lane_concurrency": {"push": 2}}, "several lanes"),
        ({"lane": None, "lane_concurrency": {"push": 2}}, "several lanes"),
        ({"lane": ["push", "mail"], "lane_rates": {"mail": 0.5}}, "several lanes"),
        ({"lane": "push", "lane_concurrency": {"push": 2}, "batch_size": 5}, "drop batch_size"),
        (
            {"lane": "push", "lane_rates": {"push": 1}, "lane_batch_sizes": {"push": 5}},
            "drop batch_size",
        ),
        ({"lane": "push", "lane_concurrency": {"mail": 2}}, "lane_concurrency names lane 'mail'"),
        ({"lane": "push", "lane_rates": {"mail": 2}}, "lane_rates names lane 'mail'"),
        ({"lane": "push", "lane_concurrency": {"push": 0}}, "must be a positive int"),
        ({"lane": "push", "lane_concurrency": {"push": 2.0}}, "must be a positive int"),
        ({"lane": "push", "lane_concurrency": {"push": True}}, "must be a positive int"),
        ({"lane": "push", "lane_rates": {"push": 0}}, "must be a finite number above 0"),
        ({"lane": "push", "lane_rates": {"push": -1.5}}, "must be a finite number above 0"),
        ({"lane": "push", "lane_rates": {"push": float("inf")}}, "must be a finite number above 0"),
        ({"lane": "push", "lane_rates": {"push": float("nan")}}, "must be a finite number above 0"),
        ({"lane": "push", "lane_rates": {"push": "5"}}, "must be a finite number above 0"),
        ({"lane": "push", "lane_rates": {"push": False}}, "must be a finite number above 0"),
        ({"lane": "push", "lane_rates": {"push": True}}, "must be a finite number above 0"),
    ],
)
def test_a_threaded_lane_the_relay_cannot_serve_is_refused_at_start(
    lanes: None, kwargs: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        run_relay(worker_id="w1", passes=0, **kwargs, **UNSAFE)


def test_every_lane_is_one_lane_threaded_when_the_project_has_only_one(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(registry, "lanes", lambda: ("mail",))
    threads = lane_threads(
        None, lane_concurrency={"mail": 2}, lane_rates=None, batch_size=None, lane_batch_sizes=None
    )
    assert threads == ("mail", 2, None)


def test_one_worker_and_no_rate_is_todays_loop_and_keeps_its_batch_size(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    """N=1 is not threaded, so a batch size and several lanes stay legal beside it."""
    _fire(2)
    counts = run_relay(
        worker_id="w1",
        passes=1,
        lane=["push", "mail"],
        batch_size=5,
        lane_concurrency={"push": 1, "mail": 1},
        **UNSAFE,
    )
    assert counts == {DeliveryStatus.SUCCEEDED: 2}
    assert inline.made == []


def test_a_rate_with_one_worker_takes_the_threaded_loop(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    _fire(1)
    clock = _Clock()
    counts = run_relay(
        worker_id="w1",
        passes=2,
        lane="mail",
        lane_rates={"mail": 10},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert counts == {DeliveryStatus.SUCCEEDED: 1}
    assert [made._n for made in inline.made] == [1]


_TENANT: ContextVar[str] = ContextVar("tenant", default="none")


def test_each_row_runs_in_a_copy_of_the_context_the_relay_was_started_in(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    _fire(2)
    seen: list[str] = []

    def mail(evt: Mail) -> None:
        seen.append(_TENANT.get())
        _TENANT.set(f"row {evt.value}")

    clock = _Clock()
    token = _TENANT.set("started")
    try:
        with _mail_says(mail):
            run_relay(
                worker_id="w1",
                passes=1,
                lane="mail",
                lane_concurrency={"mail": 2},
                now=clock,
                sleep=clock.sleep,
                wait=clock.wait,
                monotonic=clock.monotonic,
                **UNSAFE,
            )
    finally:
        _TENANT.reset(token)
    assert seen == ["started", "started"]


# --- Claims: the free workers, never a prefetch -------------------------------


def test_a_claim_takes_only_the_free_workers_and_leaves_out_the_rows_in_flight(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    inline.prompt = False
    first, second, third = _fire(3)
    made = _claims(monkeypatch)
    clock = _Clock()
    counts = run_relay(
        worker_id="w1",
        passes=3,
        lane="mail",
        lane_concurrency={"mail": 2},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert [(claim["limit"], claim["exclude_ids"]) for claim in made] == [
        (2, []),
        (2, []),
    ]
    assert sorted(CALLS) == [0, 1, 2]
    assert counts == {DeliveryStatus.SUCCEEDED: 3}
    assert {first, second, third} == set(
        DeliveryRecord.objects.filter(status=DeliveryStatus.SUCCEEDED).values_list("pk", flat=True)
    )


def test_a_refill_while_rows_are_in_flight_excludes_them(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lapsed in-flight row is claimable, so every refill names the rows it holds."""
    inline.prompt = False
    first, second = _fire(2)
    made = _claims(monkeypatch)
    clock = _Clock()
    run_relay(
        worker_id="w1",
        passes=2,
        lane="mail",
        lane_concurrency={"mail": 3},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert [(claim["limit"], claim["exclude_ids"]) for claim in made] == [
        (3, []),
        (1, [first, second]),
    ]


def test_a_rate_claims_one_row_at_a_time_spaced_on_the_monotonic_clock(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    _fire(4)
    made = _claims(monkeypatch)
    clock = _Clock()
    starts: list[float] = []
    with _mail_says(lambda evt: starts.append(clock.monotonic())):
        run_relay(
            worker_id="w1",
            passes=12,
            lane="mail",
            lane_concurrency={"mail": 3},
            lane_rates={"mail": 2},
            now=clock,
            sleep=clock.sleep,
            wait=clock.wait,
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert {claim["limit"] for claim in made} == {1}
    assert starts == [0.0, 0.5, 1.0, 1.5]


class _Enqueued:
    """A task backend that keeps the monotonic moment each id was handed to it."""

    clock: _Clock | None = None
    at: list[float] = []

    def enqueue(self, delivery_id: int, **claim: object) -> None:
        assert _Enqueued.clock is not None
        _Enqueued.at.append(_Enqueued.clock.monotonic())


def test_a_task_enqueue_counts_against_the_rate(
    lanes: None, inline: type[_InlineWorkers], settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": f"{__name__}._Enqueued"}
    _fire(3)
    clock = _Clock()
    monkeypatch.setattr(_Enqueued, "clock", clock)
    monkeypatch.setattr(_Enqueued, "at", [])
    with _mail_has("site", "task"):
        run_relay(
            worker_id="w1",
            passes=10,
            lane="mail",
            lane_concurrency={"mail": 3},
            lane_rates={"mail": 2},
            now=clock,
            sleep=clock.sleep,
            wait=clock.wait,
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert _Enqueued.at == [0.0, 0.5, 1.0]
    assert CALLS == []


def test_a_slow_rate_sleeps_in_slices_that_let_a_stop_through(
    lanes: None, inline: type[_InlineWorkers], settings: Any
) -> None:
    """At one a minute a single sleep would outlast the 30 s a stop is given."""
    settings.DJANGO_DOMAIN_EVENTS = {"POLL_SECONDS": 1.0}
    _fire(2)
    clock = _Clock()
    run_relay(
        worker_id="w1",
        passes=5,
        lane="mail",
        lane_rates={"mail": 1 / 60},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert CALLS == [0]
    assert clock.sleeps == [1.0, 1.0, 1.0, 1.0]


# --- Deferral: the lane pauses, the unstarted rows go back -------------------


def test_a_deferral_mid_claim_gives_back_exactly_the_unstarted_rows(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    first, second, third, fourth = _fire(4)

    def mail(evt: Mail) -> None:
        CALLS.append(evt.value)
        raise RetryAfter(3600, reason="quota", counts=False)

    clock = _Clock()
    paused: list[datetime] = []
    with _mail_says(mail):
        run_relay(
            worker_id="w1",
            passes=3,
            lane="mail",
            lane_concurrency={"mail": 3},
            now=clock,
            sleep=clock.sleep,
            wait=lambda t: (
                paused.extend(LanePauseRecord.objects.values_list("until", flat=True)) or False
            ),
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert CALLS == [0]
    assert [_given_back(pk) for pk in (second, third)] == [True, True]
    assert _untouched(fourth)
    assert not _given_back(first)
    assert paused == [in_project_time(clock() + timedelta(hours=1))] * 2
    assert not LanePauseRecord.objects.exists()


def test_a_pause_keeps_the_latest_end_its_deferrals_asked_for(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    """Rows in flight report their own deferrals; a shorter one never shortens the pause."""
    inline.prompt = False
    _fire(2)

    def mail(evt: Mail) -> None:
        raise RetryAfter(3600 if evt.value == 0 else 60, reason="quota", counts=False)

    clock = _Clock()
    seen: list[datetime] = []
    with _mail_says(mail):
        run_relay(
            worker_id="w1",
            passes=3,
            lane="mail",
            lane_concurrency={"mail": 2},
            now=clock,
            sleep=clock.sleep,
            wait=lambda t: (
                seen.extend(LanePauseRecord.objects.values_list("until", flat=True)) or False
            ),
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert seen == [in_project_time(clock() + timedelta(hours=1))]


def test_a_longer_deferral_in_flight_extends_the_pause(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    inline.prompt = False
    _fire(2)

    def mail(evt: Mail) -> None:
        raise RetryAfter(60 if evt.value == 0 else 3600, reason="quota", counts=False)

    clock = _Clock()
    seen: list[datetime] = []
    with _mail_says(mail):
        run_relay(
            worker_id="w1",
            passes=3,
            lane="mail",
            lane_concurrency={"mail": 2},
            now=clock,
            sleep=clock.sleep,
            wait=lambda t: (
                seen.extend(LanePauseRecord.objects.values_list("until", flat=True)) or False
            ),
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert seen == [in_project_time(clock() + timedelta(hours=1))]


def test_a_pause_that_ends_is_forgotten_and_the_lane_taken_up_again(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    _fire(2)

    def mail(evt: Mail) -> None:
        CALLS.append(evt.value)
        if len(CALLS) == 1:
            raise RetryAfter(5, reason="quota", counts=False)

    clock = _Clock(step=6)
    with _mail_says(mail):
        counts = run_relay(
            worker_id="w1",
            passes=4,
            lane="mail",
            lane_concurrency={"mail": 2},
            now=clock,
            sleep=clock.sleep,
            wait=clock.wait,
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert CALLS[0] == 0 and sorted(CALLS[1:]) == [0, 1]
    assert counts[DeliveryStatus.SUCCEEDED] == 2
    assert not LanePauseRecord.objects.exists()


def test_a_shared_pause_is_read_at_most_once_a_second(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    _fire(1)
    clock = _Clock(step=0.4)
    LanePauseRecord.objects.create(
        lane="mail",
        worker_id="other",
        paused_at=in_project_time(clock()),
        until=in_project_time(clock() + timedelta(seconds=2)),
    )
    reads: list[float] = []
    real = run_relay_module.lane_pauses

    def spy(lanes: Any, moment: datetime) -> dict[str, datetime]:
        reads.append(clock.monotonic())
        return real(lanes, moment)

    monkeypatch.setattr(run_relay_module, "lane_pauses", spy)
    run_relay(
        worker_id="w1",
        passes=8,
        lane="mail",
        lane_concurrency={"mail": 2},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert reads == pytest.approx([0.0, 1.2, 2.4])
    assert CALLS == [0]


def test_a_resumed_lane_drops_this_relays_own_pause_at_the_next_read(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    _fire(2)

    def mail(evt: Mail) -> None:
        CALLS.append(evt.value)
        if len(CALLS) == 1:
            raise RetryAfter(3600, reason="quota", counts=False)

    clock = _Clock()

    def resume(timeout: float) -> bool:
        LanePauseRecord.objects.all().delete()
        clock.advance(1.5)
        return False

    with _mail_says(mail):
        run_relay(
            worker_id="w1",
            passes=4,
            lane="mail",
            lane_concurrency={"mail": 2},
            now=clock,
            sleep=clock.sleep,
            wait=resume,
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert CALLS == [0, 1]


# --- Stop: the unstarted go back, the started finish --------------------------


def test_a_stop_mid_claim_gives_back_the_unstarted_rows(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    first, second, third = _fire(3)
    clock = _Clock()
    counts = run_relay(
        worker_id="w1",
        lane="mail",
        lane_concurrency={"mail": 3},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        stop=lambda: len(CALLS) > 0,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert counts == {DeliveryStatus.SUCCEEDED: 1}
    assert CALLS == [0]
    assert [_given_back(pk) for pk in (second, third)] == [True, True]


def test_a_stop_waits_for_the_rows_in_flight_up_to_their_lease(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    inline.prompt = False
    _fire(2)
    clock = _Clock()
    before = time.monotonic()
    counts = run_relay(
        worker_id="w1",
        passes=1,
        lane="mail",
        lane_concurrency={"mail": 2},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert counts == {DeliveryStatus.SUCCEEDED: 2}
    (deadline,) = inline.made[0].deadlines
    assert before + 33 - 1 < deadline <= time.monotonic() + 33


def test_an_exception_on_the_relay_thread_stops_the_workers_without_waiting(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    """The second signal's ``SystemExit`` must not wait out a lease."""
    inline.prompt = False
    _fire(2)
    clock = _Clock()
    reads = iter([False, False, True])

    def stop() -> bool:
        if next(reads):
            raise KeyboardInterrupt
        return False

    with pytest.raises(KeyboardInterrupt):
        run_relay(
            worker_id="w1",
            lane="mail",
            lane_concurrency={"mail": 2},
            now=clock,
            sleep=clock.sleep,
            wait=clock.wait,
            stop=stop,
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    (deadline,) = inline.made[0].deadlines
    assert deadline <= time.monotonic()


def test_a_worker_that_raised_gives_back_the_unstarted_rows_and_its_exception_ends_the_relay(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    first, second, third = _fire(3)

    def mail(evt: Mail) -> None:
        CALLS.append(evt.value)
        raise _Boom

    clock = _Clock()
    with _mail_says(mail), pytest.raises(_Boom):
        run_relay(
            worker_id="w1",
            lane="mail",
            lane_concurrency={"mail": 3},
            now=clock,
            sleep=clock.sleep,
            wait=clock.wait,
            monotonic=clock.monotonic,
            **UNSAFE,
        )
    assert CALLS == [0]
    assert [_given_back(pk) for pk in (second, third)] == [True, True]


# --- Surviving the database ---------------------------------------------------


def test_a_failed_claim_closes_the_connection_and_backs_off(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    _fire(1)
    real = run_relay_module.claim_batch
    failures = iter([True])

    def flaky(**kwargs: Any) -> list[int]:
        if next(failures, False):
            raise OperationalError("the database went away")
        return real(**kwargs)

    closed: list[bool] = []
    monkeypatch.setattr(run_relay_module, "claim_batch", flaky)
    monkeypatch.setattr(
        run_relay_module, "_close_after", lambda exc, conn, worker: closed.append(True)
    )
    clock = _Clock()
    counts = run_relay(
        worker_id="w1",
        passes=2,
        lane="mail",
        lane_concurrency={"mail": 2},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert closed == [True]
    assert len(clock.sleeps) == 1
    assert counts == {DeliveryStatus.SUCCEEDED: 1}


def test_a_failed_pause_read_is_survived_with_the_pauses_already_held(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    _fire(1)

    def unreadable(lanes: Any, moment: datetime) -> dict[str, datetime]:
        raise OperationalError("the database went away")

    monkeypatch.setattr(run_relay_module, "lane_pauses", unreadable)
    clock = _Clock()
    counts = run_relay(
        worker_id="w1",
        passes=2,
        lane="mail",
        lane_concurrency={"mail": 2},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert counts == {DeliveryStatus.SUCCEEDED: 1}


def test_an_idle_threaded_lane_sweeps_on_its_interval_and_waits(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch, settings: Any
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": 10}
    sweeps: list[float] = []
    clock = _Clock(step=6)
    monkeypatch.setattr(
        run_relay_module,
        "_sweep_or_survive",
        lambda worker, conn, stop: sweeps.append(clock.monotonic()),
    )
    run_relay(
        worker_id="w1",
        passes=4,
        lane="mail",
        lane_concurrency={"mail": 2},
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **UNSAFE,
    )
    assert sweeps == [12.0]
    assert clock.waits == 4


# --- Real threads, on a backend that skips locked rows -----------------------


def _defer_the_first_for_an_hour(evt: Mail) -> None:
    CALLS.append(evt.value)
    if evt.value == 0:
        raise RetryAfter(3600, reason="quota", counts=False)


def _run_on(clock: _Clock, passes: int, **kwargs: Any) -> dict[DeliveryStatus, int]:
    return run_relay(
        worker_id="w1",
        passes=passes,
        lane="mail",
        now=clock,
        sleep=clock.sleep,
        wait=clock.wait,
        monotonic=clock.monotonic,
        **{"lane_concurrency": {"mail": 2}, **kwargs},
        **UNSAFE,
    )


def test_this_relays_recorded_pause_holds_past_the_next_shared_reads(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    _fire(2)
    clock = _Clock(step=2)
    with _mail_says(_defer_the_first_for_an_hour):
        _run_on(clock, passes=6)
    assert CALLS == [0]
    assert clock.mono > 2 * run_relay_module._PAUSE_READ_SECONDS


def test_a_pause_whose_write_failed_still_holds_this_relay(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(run_relay_module, "_record_pause_or_survive", lambda *args: False)
    _fire(2)
    clock = _Clock(step=2)
    with _mail_says(_defer_the_first_for_an_hour):
        _run_on(clock, passes=6)
    assert CALLS == [0]
    assert not LanePauseRecord.objects.exists()


def test_another_relays_pause_holds_a_threaded_lane(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    _fire(1)
    clock = _Clock(step=2)
    LanePauseRecord.objects.create(
        lane="mail",
        worker_id="other",
        paused_at=in_project_time(clock()),
        until=in_project_time(clock() + timedelta(hours=1)),
    )
    _run_on(clock, passes=4)
    assert CALLS == []


def test_a_failed_second_pause_write_still_forgets_the_first_at_the_end(
    lanes: None, inline: type[_InlineWorkers], monkeypatch: pytest.MonkeyPatch
) -> None:
    inline.prompt = False
    _fire(2)
    real = run_relay_module._record_pause_or_survive
    writes = iter([real, lambda *args: False])
    monkeypatch.setattr(
        run_relay_module, "_record_pause_or_survive", lambda *args: next(writes)(*args)
    )

    def mail(evt: Mail) -> None:
        raise RetryAfter(60 if evt.value == 0 else 3600, reason="quota", counts=False)

    clock = _Clock()
    with _mail_says(mail):
        _run_on(clock, passes=3)
    assert not LanePauseRecord.objects.exists()


def test_a_stop_waits_for_the_lease_of_the_last_row_claimed(
    lanes: None, inline: type[_InlineWorkers]
) -> None:
    inline.prompt = False
    _fire(2)
    clock = _Clock()
    before = time.monotonic()
    _run_on(clock, passes=3, lane_rates={"mail": 1})
    assert clock.sleeps == [1.0]
    (deadline,) = inline.made[0].deadlines
    assert deadline > before + 33 - 0.5


def _until(condition: Callable[[], bool], seconds: float = 20.0) -> Callable[[], bool]:
    """A ``stop`` true once ``condition`` holds, or after ``seconds`` so a bug cannot hang CI."""
    deadline = time.monotonic() + seconds
    return lambda: condition() or time.monotonic() > deadline


@contextmanager
def _mail_has(field: str, value: Any) -> Iterator[None]:
    """Set one field of the frozen mail receiver for the length of the block."""
    entry = registry.receiver_for_key("tests.mail")
    original = getattr(entry, field)
    object.__setattr__(entry, field, value)
    try:
        yield
    finally:
        object.__setattr__(entry, field, original)


@pytest.mark.usefixtures("skip_locked_backend")
def test_threads_a_stop_waits_for_every_started_row(lanes: None) -> None:
    pks = _fire(6)
    started = threading.Event()

    def mail(evt: Mail) -> None:
        started.set()
        time.sleep(0.3)
        CALLS.append(evt.value)

    with _mail_says(mail):
        counts = run_relay(
            worker_id="w1", lane="mail", lane_concurrency={"mail": 3}, stop=started.is_set
        )
    rows = DeliveryRecord.objects.filter(pk__in=pks)
    done = {row.pk for row in rows if row.status == DeliveryStatus.SUCCEEDED}
    assert counts == {DeliveryStatus.SUCCEEDED: len(CALLS)}
    assert len(done) == len(CALLS) >= 1
    assert all(_given_back(row.pk) or _untouched(row.pk) for row in rows if row.pk not in done)


@pytest.mark.usefixtures("skip_locked_backend")
def test_threads_a_deferral_leaves_every_row_started_or_claimable(lanes: None) -> None:
    pks = _fire(6)

    def mail(evt: Mail) -> None:
        CALLS.append(evt.value)
        if len(CALLS) == 1:
            raise RetryAfter(3600, reason="quota", counts=False)
        time.sleep(0.3)

    with _mail_says(mail):
        run_relay(
            worker_id="w1",
            lane="mail",
            lane_concurrency={"mail": 3},
            stop=_until(lambda: LanePauseRecord.objects.exists(), seconds=5),
            passes=200,
        )
    called = {pks[value] for value in CALLS}
    assert 1 <= len(CALLS) <= 3
    assert len(called) == len(CALLS)
    assert all(_given_back(pk) or _untouched(pk) for pk in set(pks) - called)


@pytest.mark.usefixtures("skip_locked_backend")
def test_threads_no_row_lapses_at_ten_workers_and_five_a_second(lanes: None) -> None:
    pks = _fire(12)
    starts: list[float] = []
    lock = threading.Lock()

    def mail(evt: Mail) -> None:
        with lock:
            starts.append(time.monotonic())
        time.sleep(0.4)
        CALLS.append(evt.value)

    gated: list[float] = []
    real_started = LaneRate.started

    def started(gate: LaneRate) -> None:
        gated.append(time.monotonic())
        real_started(gate)

    with (
        _mail_has("lease_seconds", 1),
        _mail_says(mail),
        pytest.MonkeyPatch.context() as patch,
    ):
        patch.setattr(LaneRate, "started", started)
        counts = run_relay(
            worker_id="w1",
            lane="mail",
            lane_concurrency={"mail": 10},
            lane_rates={"mail": 5},
            stop=_until(lambda: len(CALLS) == 12),
        )
    rows = list(DeliveryRecord.objects.filter(pk__in=pks))
    assert counts == {DeliveryStatus.SUCCEEDED: 12}
    assert sorted(CALLS) == list(range(12))
    assert {(row.status, row.attempts) for row in rows} == {(DeliveryStatus.SUCCEEDED, 1)}
    starts.sort()
    gaps = [later - earlier for earlier, later in zip(gated, gated[5:], strict=False)]
    assert min(gaps) >= 1.0
    assert min(later - earlier for earlier, later in zip(starts, starts[5:], strict=False)) >= 0.9


@pytest.mark.usefixtures("skip_locked_backend")
def test_threads_a_row_outliving_its_lease_is_not_reclaimed_while_in_flight(
    lanes: None, settings: Any
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"POLL_SECONDS": 0.1}
    (pk,) = _fire(1)

    def mail(evt: Mail) -> None:
        CALLS.append(evt.value)
        time.sleep(1.8)

    with _mail_has("lease_seconds", 1), _mail_says(mail):
        counts = run_relay(
            worker_id="w1",
            lane="mail",
            lane_concurrency={"mail": 2},
            stop=_until(lambda: len(CALLS) == 1 and _settled(pk)),
        )
    assert CALLS == [0]
    assert counts == {DeliveryStatus.SUCCEEDED: 1}


@pytest.mark.usefixtures("skip_locked_backend")
def test_threads_a_lapse_is_reported_on_the_main_thread_and_rows_run_on_workers(
    lanes: None,
) -> None:
    lapsed, fresh = _fire(2)
    long_ago = django_timezone.now() - timedelta(hours=1)
    DeliveryRecord.objects.filter(pk=lapsed).update(
        status=DeliveryStatus.CLAIMED,
        claimed_by="dead",
        claimed_at=long_ago,
        lease_expires_at=long_ago,
        available_at=long_ago,
        attempts=5,
    )
    told: list[threading.Thread] = []
    ran: list[threading.Thread] = []
    tell = lambda failure: told.append(threading.current_thread())  # noqa: E731
    with (
        _mail_has("on_failure", tell),
        _mail_says(lambda e: ran.append(threading.current_thread())),
    ):
        run_relay(
            worker_id="w1",
            lane="mail",
            lane_concurrency={"mail": 2},
            stop=_until(lambda: _settled(fresh)),
        )
    assert DeliveryRecord.objects.get(pk=lapsed).status == DeliveryStatus.DEAD
    assert told == [threading.main_thread()]
    assert len(ran) == 1 and ran[0] is not threading.main_thread()
