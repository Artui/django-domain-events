from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from typing import Any

import pytest
from django.core.management import call_command
from django.db import OperationalError, connections, transaction
from django.utils import timezone as django_timezone

from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.lane_pause_record import LanePauseRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import in_project_time
from tests.conftest import event_registered, receiver_registered

pytestmark = pytest.mark.django_db(transaction=True)

UNSAFE = {"allow_unsafe_concurrency": True}

CALLS: list[str] = []
"""Each call of the mail receiver, as the name of the thread that made it."""


@dataclass(frozen=True, slots=True)
class Mail:
    value: int


class _Runaway(BaseException):
    """A BaseException so the relay, which survives any ``Exception``, lets it out."""


def _mail(defers: int) -> RegisteredReceiver:
    """A mail-lane receiver whose first ``defers`` calls defer an hour without counting."""

    def mail(evt: Mail) -> None:
        CALLS.append(threading.current_thread().name)
        if len(CALLS) > 10:
            raise _Runaway("the paused lane kept being attempted")
        if len(CALLS) <= defers:
            raise RetryAfter(3600, reason="quota", counts=False)

    return RegisteredReceiver(
        key="tests.mail",
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


@pytest.fixture
def lane() -> Iterator[Callable[[int], None]]:
    """Registers ``Mail``; the test registers its receiver through the callable."""
    CALLS.clear()
    with event_registered(Mail, "tests.Mail"), ExitStack() as stack:
        yield lambda defers: stack.enter_context(receiver_registered(_mail(defers)))
    CALLS.clear()


def _fire(count: int) -> None:
    with transaction.atomic():
        for value in range(count):
            fire(Mail(value=value))


class _Clock:
    """An injected clock that each idle wait moves on by ``step`` seconds."""

    def __init__(self, step: float = 0.0) -> None:
        self.at = django_timezone.now()
        self.step = step

    def __call__(self) -> datetime:
        return self.at

    def wait(self, timeout: float) -> bool:
        self.at += timedelta(seconds=self.step)
        return False


def _mail_claims(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The worker of every claim of the mail lane."""
    made: list[str] = []
    real = run_relay_module.claim_batch

    def spy(**kwargs: Any) -> list[int]:
        if kwargs["lane"] == "mail":
            made.append(kwargs["worker_id"])
        return real(**kwargs)

    monkeypatch.setattr(run_relay_module, "claim_batch", spy)
    return made


def _pause(worker_id: str, at: datetime, hours: float) -> None:
    LanePauseRecord.objects.create(
        lane="mail",
        worker_id=worker_id,
        paused_at=in_project_time(at),
        until=in_project_time(at + timedelta(hours=hours)),
    )


@pytest.mark.usefixtures("skip_locked_backend")
def test_relay_b_skips_a_lane_relay_a_paused_until_the_pause_ends(
    lane: Callable[[int], None],
) -> None:
    """A defers on its own connection and idles; B on another never calls the receiver."""
    lane(1)
    _fire(3)
    a_paused = threading.Event()
    done = threading.Event()
    errors: list[BaseException] = []

    def a_wait(timeout: float) -> bool:
        a_paused.set()
        done.wait(timeout=30)
        return False

    def relay_a() -> None:
        try:
            run_relay(worker_id="A", lane="mail", wait=a_wait, stop=done.is_set)
        except BaseException as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=relay_a, name="A")
    thread.start()
    try:
        assert a_paused.wait(timeout=30), "relay A never paused"
        b_clock = _Clock(step=600)
        run_relay(worker_id="B", lane="mail", passes=3, now=b_clock, wait=b_clock.wait)
        assert CALLS == ["A"], "relay B called a lane relay A had paused"
        assert not DeliveryRecord.objects.filter(claimed_by="B").exists()

        after = _Clock()
        after.at += timedelta(hours=2)
        run_relay(worker_id="B", lane="mail", passes=1, now=after, wait=after.wait)
        assert CALLS == ["A"] + ["MainThread"] * 3, "relay B did not resume after the pause"
    finally:
        done.set()
        thread.join(timeout=30)
    assert errors == []
    assert not thread.is_alive()
    assert not LanePauseRecord.objects.exists(), "relay A returned and left its record"


def test_a_relay_skips_a_lane_another_relay_recorded_without_a_query_of_it(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The record is read once per pass; while it holds, the lane is not claimed."""
    lane(0)
    _fire(1)
    clock = _Clock(step=1800)
    _pause("other:1", clock.at, hours=1)
    claims = _mail_claims(monkeypatch)

    run_relay(worker_id="w1", lane="mail", passes=4, now=clock, wait=clock.wait, **UNSAFE)

    assert claims == ["w1"] * 2, "the lane was claimed while another relay held it paused"
    assert CALLS == ["MainThread"]


def test_the_latest_of_several_pauses_of_a_lane_holds_it(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The clock moves ten seconds a reading, so the one-second pause has ended at the claim."""
    lane(0)
    _fire(1)
    start = django_timezone.now()
    _pause("other:1", start, hours=1)
    _pause("other:2", start, hours=1 / 3600)
    readings = iter(start + timedelta(seconds=10 * n) for n in range(100))
    claims = _mail_claims(monkeypatch)

    run_relay(
        worker_id="w1",
        lane="mail",
        passes=1,
        now=lambda: next(readings),
        wait=lambda _: False,
        **UNSAFE,
    )

    assert claims == [], "the lane resumed when the shorter pause ended"


def test_a_relay_serving_every_lane_reads_the_pauses_of_every_lane(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    lane(0)
    _fire(1)
    clock = _Clock()
    _pause("other:1", clock.at, hours=1)
    claims = _mail_claims(monkeypatch)

    run_relay(worker_id="w1", lane=None, passes=2, now=clock, wait=clock.wait, **UNSAFE)

    assert claims == []
    assert CALLS == []


def test_pause_lane_stops_a_relay_and_its_record_outlives_the_relay(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    lane(0)
    _fire(1)
    call_command("pause_lane", "mail", "--for", "3600", stdout=StringIO())
    claims = _mail_claims(monkeypatch)

    run_relay(worker_id="w1", lane="mail", passes=3, wait=lambda _: False, **UNSAFE)

    assert claims == []
    assert list(LanePauseRecord.objects.values_list("worker_id", flat=True)) == ["operator"]


def test_resume_lane_restarts_the_relay_that_recorded_the_pause_within_one_poll(
    lane: Callable[[int], None],
) -> None:
    """Pass one defers, pass two idles and the resume runs, pass three delivers."""
    lane(1)
    _fire(2)
    clock = _Clock(step=1)

    def wait(timeout: float) -> bool:
        call_command("resume_lane", "mail", stdout=StringIO())
        return clock.wait(timeout)

    counts = run_relay(worker_id="w1", lane="mail", passes=3, now=clock, wait=wait, **UNSAFE)

    assert counts == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 1}
    assert len(CALLS) == 2, "the relay held its own pause after the resume"


def test_resume_lane_restarts_a_relay_an_operator_paused_within_one_poll(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    lane(0)
    _fire(1)
    call_command("pause_lane", "mail", "--for", "3600", stdout=StringIO())
    claims = _mail_claims(monkeypatch)

    def wait(timeout: float) -> bool:
        call_command("resume_lane", "mail", stdout=StringIO())
        return False

    run_relay(worker_id="w1", lane="mail", passes=2, wait=wait, **UNSAFE)

    assert claims == ["w1"]
    assert CALLS == ["MainThread"]


def test_a_pause_whose_record_was_never_written_still_holds_the_relay(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only a record this relay wrote and then saw gone is read as a resume."""
    lane(10)
    _fire(3)
    clock = _Clock(step=1)

    def broken(*args: object, **kwargs: object) -> object:
        raise OperationalError("the connection is lost")

    monkeypatch.setattr(LanePauseRecord.objects, "update_or_create", broken)
    run_relay(worker_id="w1", lane="mail", passes=3, now=clock, wait=clock.wait, **UNSAFE)

    assert len(CALLS) == 1, "the lane was attempted while its unrecorded pause held"


def test_a_pause_whose_record_was_never_written_ends_on_the_relays_clock(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no record to see expire, the relay's own map ends the pause."""
    lane(1)
    _fire(2)
    clock = _Clock(step=3600)

    def broken(*args: object, **kwargs: object) -> object:
        raise OperationalError("the connection is lost")

    monkeypatch.setattr(LanePauseRecord.objects, "update_or_create", broken)
    counts = run_relay(worker_id="w1", lane="mail", passes=3, now=clock, wait=clock.wait, **UNSAFE)

    assert counts == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 1}, "it stayed paused"


def test_a_pause_that_ends_between_the_read_and_the_lanes_turn(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each stop check moves the clock two seconds, so the pause ends after the pass's read."""
    monkeypatch.setattr("django_domain_events.delivery.deliver.random.random", lambda: 0.5)
    lane(1)
    _fire(2)
    clock = _Clock(step=3597)

    def stop() -> bool:
        clock.at += timedelta(seconds=2)
        return False

    counts = run_relay(
        worker_id="w1", lane="mail", passes=4, now=clock, wait=clock.wait, stop=stop, **UNSAFE
    )

    assert counts == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 1}


def test_resume_lane_restarts_a_relay_whose_cleanup_of_passed_pauses_failed(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Its own record was written, so lifting it lifts the relay's pause too."""
    lane(1)
    _fire(2)
    clock = _Clock(step=1)
    real_filter = LanePauseRecord.objects.filter

    def failing_cleanup(*args: object, **kwargs: object) -> object:
        if "until__lte" in kwargs:
            raise OperationalError("the connection is lost")
        return real_filter(*args, **kwargs)

    def wait(timeout: float) -> bool:
        call_command("resume_lane", "mail", stdout=StringIO())
        return clock.wait(timeout)

    monkeypatch.setattr(LanePauseRecord.objects, "filter", failing_cleanup)
    counts = run_relay(worker_id="w1", lane="mail", passes=3, now=clock, wait=wait, **UNSAFE)

    assert counts == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 1}
    assert "relay w1 could not delete the pauses that have passed" in caplog.text


def test_pauses_that_cannot_be_read_do_not_stop_the_relay_or_lift_its_own(
    lane: Callable[[int], None], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The read fails from the second pass on: the relay goes on, paused as it was."""
    lane(10)
    _fire(3)
    clock = _Clock()
    real_filter = LanePauseRecord.objects.filter
    reads: list[None] = []

    def failing_reads(*args: object, **kwargs: object) -> object:
        if "until__gt" in kwargs:
            reads.append(None)
            if len(reads) > 1:
                raise OperationalError("the connection is lost")
        return real_filter(*args, **kwargs)

    monkeypatch.setattr(LanePauseRecord.objects, "filter", failing_reads)
    with caplog.at_level(logging.ERROR, logger="django_domain_events.delivery.run_relay"):
        run_relay(worker_id="w1", lane="mail", passes=3, now=clock, wait=clock.wait, **UNSAFE)

    assert len(reads) == 3
    assert len(CALLS) == 1, "a failed read lifted the relay's own pause"
    assert caplog.text.count("relay w1 could not read the lane pauses") == 2
    assert write_alias() in connections
