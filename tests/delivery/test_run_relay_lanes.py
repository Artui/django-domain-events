from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest
from django.db import OperationalError, connections, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone as django_timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.lane_pause_record import LanePauseRecord
from django_domain_events.settings import setting
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import in_project_time
from tests.conftest import event_registered, receiver_registered, receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

UNSAFE = {"allow_unsafe_concurrency": True}

ORDER: list[str] = []
"""Each delivery's lane, in the order the relay delivered them."""


@dataclass(frozen=True, slots=True)
class Push:
    value: int


@dataclass(frozen=True, slots=True)
class Sms:
    value: int


class _Runaway(BaseException):
    """Raised by a receiver called more often than a working pause allows, so a
    broken pause fails the test rather than hanging it. A BaseException because
    the relay swallows every ``Exception`` a delivery raises."""


def _lane_receiver(lane: str, event_class: type, **options: Any) -> RegisteredReceiver:
    options.setdefault("func", lambda evt: ORDER.append(lane))
    return RegisteredReceiver(
        key=f"tests.{lane}",
        event_class=event_class,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane=lane,
        **options,
    )


@pytest.fixture
def lanes() -> Iterator[None]:
    """``push`` and ``sms``, each with its own lease, and ``sms`` able to defer
    without counting, which needs ``give_up_after``."""
    ORDER.clear()
    with (
        event_registered(Push, "tests.Push"),
        event_registered(Sms, "tests.Sms"),
        receiver_registered(_lane_receiver("push", Push, lease_seconds=60)),
        receiver_registered(
            _lane_receiver("sms", Sms, lease_seconds=30, give_up_after=timedelta(days=1))
        ),
    ):
        yield
    ORDER.clear()


def _fire(event_class: type, count: int = 1) -> None:
    with transaction.atomic():
        for value in range(count):
            fire(event_class(value=value))


class _Clock:
    """An injected clock that each idle wait moves on by ``step`` seconds."""

    def __init__(self, step: float = 0.0) -> None:
        self.at = django_timezone.now()
        self.step = step
        self.waits = 0

    def __call__(self) -> datetime:
        return self.at

    def wait(self, timeout: float) -> bool:
        self.waits += 1
        self.at += timedelta(seconds=self.step)
        return False


@contextmanager
def _claims(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[dict[str, Any]]]:
    """Every claim the relay makes, as the keyword arguments it made it with."""
    made: list[dict[str, Any]] = []
    real = run_relay_module.claim_batch

    def spy(**kwargs: Any) -> list[int]:
        made.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(run_relay_module, "claim_batch", spy)
    yield made


def _deferring(clock: _Clock, seen: list[datetime], *, times: int = 10) -> Callable[[Sms], None]:
    """An SMS receiver pausing an hour on each of its first calls, then accepting."""

    def sms(evt: Sms) -> None:
        seen.append(clock())
        if len(seen) > 10:
            raise _Runaway("the paused lane kept being attempted")
        if len(seen) <= times:
            raise RetryAfter(3600, reason="credit", counts=False)
        ORDER.append("sms")

    return sms


def _unclaimed(key: str) -> bool:
    return not DeliveryRecord.objects.filter(receiver_key=key).exclude(claimed_by="").exists()


@pytest.fixture
def closes(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every close of the write alias's connection, and still close it."""
    conn = connections[write_alias()]
    real_close = conn.close
    seen: list[str] = []

    def spy() -> None:
        seen.append(conn.alias)
        real_close()

    monkeypatch.setattr(conn, "close", spy)
    return seen


# --- Fairness: a burst costs another lane one batch, not the burst ------------


def test_a_burst_in_one_lane_does_not_hold_back_another_under_every_lane(lanes: None) -> None:
    """Sixty push rows owed before one SMS row."""
    _fire(Push, 60)
    _fire(Sms)

    run_relay(worker_id="w1", passes=1, lane=None, batch_size=10, **UNSAFE)

    assert "sms" in ORDER, "the SMS row waited behind the push burst"


def test_a_set_of_lanes_delivers_each_lane_within_one_batch_of_the_others(lanes: None) -> None:
    """The SMS row is delivered at or before position ``b_push + 1``."""
    _fire(Push, 60)
    _fire(Sms)

    run_relay(worker_id="w1", passes=1, lane=["push", "sms"], batch_size=10, **UNSAFE)

    assert ORDER.index("sms") <= 10


def test_a_lanes_wait_is_one_batch_of_each_other_lane_not_their_backlog(lanes: None) -> None:
    """push sized at three: SMS waits for three push deliveries, not a thousand."""
    _fire(Push, 1000)
    _fire(Sms, 3)

    run_relay(
        worker_id="w1",
        passes=3,
        lane=["push", "sms"],
        lane_batch_sizes={"push": 3, "sms": 1},
        **UNSAFE,
    )

    assert ORDER == ["push", "push", "push", "sms"] * 3


def test_each_lane_is_claimed_just_before_it_is_delivered(lanes: None) -> None:
    """While push delivers, no SMS row is claimed: an early batch would burn its lease."""
    claimed_meanwhile: list[int] = []

    def push(evt: Push) -> None:
        claimed_meanwhile.append(
            DeliveryRecord.objects.filter(
                receiver_key="tests.sms", status=DeliveryStatus.CLAIMED
            ).count()
        )
        ORDER.append("push")

    _fire(Push, 3)
    _fire(Sms, 3)
    with receiver_replaced("tests.push", push):
        run_relay(worker_id="w1", passes=1, lane=["push", "sms"], **UNSAFE)

    assert claimed_meanwhile == [0, 0, 0]
    assert ORDER == ["push"] * 3 + ["sms"] * 3


def test_every_lane_is_claimed_under_its_own_lease(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``push`` declares 60 seconds and ``sms`` 30."""
    _fire(Push)
    _fire(Sms)

    with _claims(monkeypatch) as made:
        run_relay(worker_id="w1", passes=1, lane=None, **UNSAFE)

    leases = {claim["lane"]: claim["lease"] for claim in made}
    assert leases.get("push") == timedelta(seconds=66)
    assert leases.get("sms") == timedelta(seconds=33)


def test_a_set_is_served_once_per_lane_in_sorted_order(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Duplicates name the same work, so a concatenated manifest serves each lane once."""
    with _claims(monkeypatch) as made:
        run_relay(
            worker_id="w1", passes=1, lane=["sms", "push", "sms"], wait=lambda _: False, **UNSAFE
        )

    assert [claim["lane"] for claim in made] == ["push", "sms"]


def test_every_lane_is_reread_at_each_pass(lanes: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """A lane declared while the relay runs is served from its next pass."""
    late = _lane_receiver("late", Push)

    def wait(timeout: float) -> bool:
        registry.register_receiver(late)
        return False

    try:
        with _claims(monkeypatch) as made:
            run_relay(worker_id="w1", passes=2, lane=None, wait=wait, **UNSAFE)
    finally:
        registry._receivers.pop(late.key, None)

    assert [claim["lane"] for claim in made] == [
        "default",
        "push",
        "sms",
        "default",
        "late",
        "push",
        "sms",
    ]


def test_a_string_is_one_lane_not_its_letters(lanes: None) -> None:
    """A str is a collection of characters, so 'sms' would serve the lanes s and m."""
    _fire(Sms)

    assert run_relay(worker_id="w1", passes=1, lane="sms", **UNSAFE) == {
        DeliveryStatus.SUCCEEDED: 1
    }


def test_lane_sizes_override_the_batch_size_for_the_lanes_they_name(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _claims(monkeypatch) as made:
        run_relay(
            worker_id="w1",
            passes=1,
            lane=["push", "sms"],
            batch_size=7,
            lane_batch_sizes={"sms": 2},
            wait=lambda _: False,
            **UNSAFE,
        )

    assert [(claim["lane"], claim["limit"]) for claim in made] == [("push", 7), ("sms", 2)]


def test_under_every_lane_a_size_may_name_any_declared_lane(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _claims(monkeypatch) as made:
        run_relay(
            worker_id="w1",
            passes=1,
            lane=None,
            lane_batch_sizes={"push": 3},
            wait=lambda _: False,
            **UNSAFE,
        )

    assert ("push", 3) in [(claim["lane"], claim["limit"]) for claim in made]


# --- Refusals ------------------------------------------------------------------


def test_an_undeclared_member_is_refused_by_name(lanes: None) -> None:
    with pytest.raises(ValueError, match="No receiver is declared in lane 'smss'"):
        run_relay(worker_id="w1", passes=1, lane=["push", "smss"], **UNSAFE)


def test_an_empty_set_of_lanes_is_refused(lanes: None) -> None:
    """Neither every lane nor any lane: such a relay would claim nothing."""
    with pytest.raises(ValueError, match="empty collection"):
        run_relay(worker_id="w1", passes=1, lane=[], **UNSAFE)


def test_a_lane_that_is_not_a_string_is_refused(lanes: None) -> None:
    with pytest.raises(ValueError, match="A lane is named by a str, got 7"):
        run_relay(worker_id="w1", passes=1, lane=["push", 7], **UNSAFE)


def test_a_size_for_a_lane_not_served_is_refused(lanes: None) -> None:
    """Declared, but not served by this relay: a manifest sizing work nothing here will claim."""
    with pytest.raises(ValueError, match="lane_batch_sizes names lane 'sms', which is not served"):
        run_relay(worker_id="w1", passes=1, lane="push", lane_batch_sizes={"sms": 2}, **UNSAFE)


def test_under_every_lane_a_size_for_an_undeclared_lane_is_refused(lanes: None) -> None:
    with pytest.raises(ValueError, match="lane_batch_sizes names lane 'smss'"):
        run_relay(worker_id="w1", passes=1, lane=None, lane_batch_sizes={"smss": 2}, **UNSAFE)


@pytest.mark.parametrize("size", [0, -1])
def test_a_lane_size_below_one_is_refused(lanes: None, size: int) -> None:
    with pytest.raises(ValueError, match=r"lane_batch_sizes\['sms'\] must be positive"):
        run_relay(
            worker_id="w1", passes=1, lane=["push", "sms"], lane_batch_sizes={"sms": size}, **UNSAFE
        )


# --- Stopping --------------------------------------------------------------------


def test_a_stop_during_one_lanes_batch_hands_it_back_and_claims_no_other(lanes: None) -> None:
    """Only the batch in hand is handed back; the lane after it was never claimed."""
    _fire(Push, 3)
    _fire(Sms, 2)

    counts = run_relay(
        worker_id="w1",
        lane=["push", "sms"],
        stop=lambda: bool(ORDER),
        wait=lambda _: False,
        **UNSAFE,
    )

    assert counts == {DeliveryStatus.SUCCEEDED: 1}
    handed_back = DeliveryRecord.objects.filter(
        receiver_key="tests.push",
        status=DeliveryStatus.CLAIMED,
        lease_expires_at__lt=django_timezone.now(),
    )
    assert handed_back.count() == 2
    assert _unclaimed("tests.sms"), "a lane after the stop was claimed"


def test_a_stop_between_lanes_claims_no_further_lane(lanes: None) -> None:
    """Set after the last push row, the stop is read before ``sms``'s turn."""
    _fire(Push, 3)
    _fire(Sms, 2)

    counts = run_relay(
        worker_id="w1",
        lane=["push", "sms"],
        stop=lambda: len(ORDER) >= 3,
        wait=lambda _: False,
        **UNSAFE,
    )

    assert counts == {DeliveryStatus.SUCCEEDED: 3}
    assert _unclaimed("tests.sms"), "the next lane was claimed after the stop"


def test_a_failed_claim_ends_the_pass(lanes: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """A database refusing one lane's claim refuses them all: the rest is not tried."""
    tried: list[str] = []

    def failing(**kwargs: Any) -> list[int]:
        tried.append(kwargs["lane"])
        raise OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(run_relay_module, "claim_batch", failing)
    waits: list[float] = []
    slept: list[float] = []

    run_relay(
        worker_id="w1",
        passes=1,
        lane=["push", "sms"],
        sleep=slept.append,
        wait=lambda t: bool(waits.append(t)),
        **UNSAFE,
    )

    assert tried == ["push"]
    assert slept == [1.0]
    assert waits == []


# --- Pauses --------------------------------------------------------------------


def test_a_paused_lane_is_not_queried_while_the_others_go_on(lanes: None) -> None:
    """SMS defers for an hour at its first row."""
    seen: list[datetime] = []
    _fire(Push, 6)
    _fire(Sms, 3)
    clock = _Clock(step=1800)

    with (
        receiver_replaced("tests.sms", _deferring(clock, seen)),
        CaptureQueriesContext(connections[write_alias()]) as queries,
    ):
        run_relay(
            worker_id="w1",
            passes=6,
            lane=["push", "sms"],
            batch_size=2,
            now=clock,
            wait=clock.wait,
            **UNSAFE,
        )

    start = seen[0]
    assert seen == [start, start + timedelta(hours=1)], "SMS was attempted while paused"
    sms_claims = [q for q in queries.captured_queries if "IN ('tests.sms')" in q["sql"]]
    assert len(sms_claims) == 2, "the paused lane was queried"
    assert ORDER == ["push"] * 6
    assert clock.waits == 2


def test_a_deferral_asks_nothing_about_the_rows_it_hands_back(lanes: None) -> None:
    """Every row in the batch is in the deferring lane, so there is no split."""
    _fire(Sms, 3)
    clock = _Clock()

    with (
        receiver_replaced("tests.sms", _deferring(clock, [])),
        CaptureQueriesContext(connections[write_alias()]) as queries,
    ):
        run_relay(worker_id="w1", passes=1, lane=None, now=clock, **UNSAFE)

    split = re.compile(r'^SELECT .*"receiver_key" FROM \S*deliveryrecord\S* WHERE .*"id" IN \(')
    assert [q["sql"] for q in queries.captured_queries if split.match(q["sql"])] == []
    assert (
        DeliveryRecord.objects.filter(
            receiver_key="tests.sms", lease_expires_at__lt=django_timezone.now()
        ).count()
        == 2
    ), "the rest of the batch was not handed back"


def _pauses() -> list[tuple[str, str, datetime, datetime]]:
    return list(
        LanePauseRecord.objects.order_by("lane", "worker_id").values_list(
            "lane", "worker_id", "paused_at", "until"
        )
    )


def test_a_deferral_records_the_pause_where_operators_can_see_it(lanes: None) -> None:
    """Read from the idle wait, since the relay deletes its records on return."""
    _fire(Sms)
    clock = _Clock()
    during: list[list[tuple[str, str, datetime, datetime]]] = []

    def wait(timeout: float) -> bool:
        during.append(_pauses())
        return False

    with receiver_replaced("tests.sms", _deferring(clock, [])):
        run_relay(worker_id="w1", passes=2, lane="sms", now=clock, wait=wait, **UNSAFE)

    at = in_project_time(clock.at)
    assert during == [[("sms", "w1", at, at + timedelta(hours=1))]]
    assert _pauses() == [], "the relay returned and left its record behind"


def test_a_pause_that_ends_deletes_its_record(lanes: None) -> None:
    """The record is seen once, then the pause ends and the next wait sees none."""
    _fire(Sms, 2)
    clock = _Clock(step=3600)
    during: list[list[tuple[str, str, datetime, datetime]]] = []

    def wait(timeout: float) -> bool:
        during.append(_pauses())
        return clock.wait(timeout)

    with receiver_replaced("tests.sms", _deferring(clock, [], times=1)):
        run_relay(worker_id="w1", passes=4, lane="sms", now=clock, wait=wait, **UNSAFE)

    assert ORDER == ["sms"], "the handed-back row was not delivered after the pause"
    assert len(during) == 2
    assert [lane for lane, *_ in during[0]] == ["sms"]
    assert during[1] == [], "the pause ended and its record stayed"


def test_a_pause_that_ends_deletes_only_its_own_lanes_record() -> None:
    """Push pauses for an hour and SMS for five."""

    def deferring_once(lane: str, hours: int) -> Callable[[Any], None]:
        calls: list[None] = []

        def receive(evt: Any) -> None:
            calls.append(None)
            if len(calls) == 1:
                raise RetryAfter(hours * 3600, reason="quota", counts=False)
            ORDER.append(lane)

        return receive

    day = timedelta(days=1)
    clocks: list[_Clock] = []
    during: list[list[str]] = []

    def wait(timeout: float) -> bool:
        during.append([lane for lane, *_ in _pauses()])
        return clocks[0].wait(timeout)

    ORDER.clear()
    with (
        event_registered(Push, "tests.Push"),
        event_registered(Sms, "tests.Sms"),
        receiver_registered(
            _lane_receiver("push", Push, give_up_after=day, func=deferring_once("push", 1))
        ),
        receiver_registered(
            _lane_receiver("sms", Sms, give_up_after=day, func=deferring_once("sms", 5))
        ),
    ):
        _fire(Push)
        _fire(Sms)
        clocks.append(_Clock(step=3600))
        run_relay(
            worker_id="w1", passes=4, lane=["push", "sms"], now=clocks[0], wait=wait, **UNSAFE
        )

    assert ORDER == ["push"]
    assert during[:2] == [["push", "sms"], ["sms"]]
    ORDER.clear()


def test_a_stop_deletes_the_records_of_lanes_still_paused(lanes: None) -> None:
    _fire(Sms)
    clock = _Clock()
    waited: list[float] = []

    with receiver_replaced("tests.sms", _deferring(clock, [])):
        run_relay(
            worker_id="w1",
            lane="sms",
            now=clock,
            stop=lambda: bool(waited),
            wait=lambda t: bool(waited.append(t)),
            **UNSAFE,
        )

    assert waited == [1.0]
    assert _pauses() == []


def test_a_stop_mid_batch_deletes_the_records_of_lanes_still_paused(lanes: None) -> None:
    """A stop mid-batch takes the pause record with the relay."""
    _fire(Sms)
    _fire(Push, 4)
    clock = _Clock()

    with receiver_replaced("tests.sms", _deferring(clock, [])):
        run_relay(
            worker_id="w1",
            lane=["push", "sms"],
            batch_size=2,
            now=clock,
            stop=lambda: len(ORDER) >= 3,
            wait=lambda _: False,
            **UNSAFE,
        )

    assert ORDER == ["push"] * 3
    assert _pauses() == []


def test_a_relay_deletes_only_its_own_records(lanes: None) -> None:
    """Another relay's unexpired record of the same lane is not this relay's to clear."""
    _fire(Sms)
    clock = _Clock()
    other = LanePauseRecord.objects.create(
        lane="sms",
        worker_id="other:2",
        paused_at=in_project_time(clock.at),
        until=in_project_time(clock.at + timedelta(hours=2)),
    )

    with receiver_replaced("tests.sms", _deferring(clock, [])):
        run_relay(worker_id="w1", passes=2, lane="sms", now=clock, wait=lambda _: False, **UNSAFE)

    assert list(LanePauseRecord.objects.values_list("pk", flat=True)) == [other.pk]


def test_a_write_deletes_every_record_whose_pause_has_passed(lanes: None) -> None:
    """A relay that died without returning leaves its record, and nobody else owns it."""
    _fire(Sms)
    clock = _Clock()
    at = in_project_time(clock.at)
    for worker_id, until in [
        ("gone:1", at - timedelta(seconds=1)),
        ("gone:2", at),
        ("alive:3", at + timedelta(seconds=1)),
    ]:
        LanePauseRecord.objects.create(
            lane="push", worker_id=worker_id, paused_at=at - timedelta(hours=1), until=until
        )
    during: list[list[str]] = []

    def wait(timeout: float) -> bool:
        during.append(sorted(LanePauseRecord.objects.values_list("worker_id", flat=True)))
        return False

    with receiver_replaced("tests.sms", _deferring(clock, [])):
        run_relay(worker_id="w1", passes=2, lane="sms", now=clock, wait=wait, **UNSAFE)

    assert during == [["alive:3", "w1"]]


def test_a_relay_restarted_under_its_worker_id_moves_its_old_record(lanes: None) -> None:
    """A relay given a fixed ``--worker-id`` that died paused left a record under that id."""
    _fire(Sms)
    clock = _Clock()
    at = in_project_time(clock.at)
    LanePauseRecord.objects.create(
        lane="sms", worker_id="w1", paused_at=at - timedelta(hours=1), until=at + timedelta(hours=5)
    )
    during: list[list[tuple[str, str, datetime, datetime]]] = []

    def wait(timeout: float) -> bool:
        during.append(_pauses())
        return False

    with receiver_replaced("tests.sms", _deferring(clock, [])):
        run_relay(worker_id="w1", passes=2, lane="sms", now=clock, wait=wait, **UNSAFE)

    assert during == [[("sms", "w1", at, at + timedelta(hours=1))]]


def test_a_pause_record_that_cannot_be_written_does_not_stop_the_relay(
    lanes: None,
    monkeypatch: pytest.MonkeyPatch,
    closes: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The record is for operators; the in-process pause still governs the relay."""
    seen: list[datetime] = []
    _fire(Sms, 3)
    clock = _Clock()

    def broken(*args: object, **kwargs: object) -> object:
        raise OperationalError("the connection is lost")

    monkeypatch.setattr(LanePauseRecord.objects, "update_or_create", broken)
    with receiver_replaced("tests.sms", _deferring(clock, seen)):
        counts = run_relay(
            worker_id="w1", passes=3, lane="sms", now=clock, wait=lambda _: False, **UNSAFE
        )

    assert counts == {DeliveryStatus.FAILED: 1}
    assert len(seen) == 1, "the lane was attempted while paused"
    assert "could not record the pause of lane sms" in caplog.text
    assert closes == [write_alias()]


def test_a_pause_record_that_cannot_be_deleted_does_not_stop_the_relay(
    lanes: None,
    monkeypatch: pytest.MonkeyPatch,
    closes: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The record stays until its until passes, and any relay's next write deletes it."""
    _fire(Sms)
    clock = _Clock()
    real_filter = LanePauseRecord.objects.filter

    def filter_failing_deletes(*args: object, **kwargs: object) -> object:
        if "worker_id" in kwargs:
            raise OperationalError("the connection is lost")
        return real_filter(*args, **kwargs)

    monkeypatch.setattr(LanePauseRecord.objects, "filter", filter_failing_deletes)
    with receiver_replaced("tests.sms", _deferring(clock, [])):
        counts = run_relay(worker_id="w1", passes=1, lane="sms", now=clock, **UNSAFE)

    assert counts == {DeliveryStatus.FAILED: 1}
    assert "could not delete its pause records" in caplog.text
    assert closes == [write_alias()]
    assert [lane for lane, *_ in _pauses()] == ["sms"]


# --- What does not change ----------------------------------------------------


@contextmanager
def _statements() -> Iterator[list[str]]:
    """The SQL sent on the write connection, as templates with placeholders."""
    sent: list[str] = []

    def capture(
        execute: Callable[..., Any], sql: str, params: Any, many: bool, context: Any
    ) -> Any:
        sent.append(sql)
        return execute(sql, params, many, context)

    with connections[write_alias()].execute_wrapper(capture):
        yield sent


def _the_pass_without_lanes(*, lane: str | None, clock: _Clock) -> None:
    """One pass as a single-lane relay ran: one claim_batch, then a delivery per row."""
    ids = claim_batch(
        worker_id="w1",
        now=clock(),
        lease=timedelta(
            seconds=registry.claim_lease_seconds(lane, default=setting("LEASE_SECONDS"))
        ),
        limit=setting("BATCH_SIZE"),
        lane=lane,
        exclude_lanes=(),
    )
    for delivery_id in ids:
        dispatch_one(delivery_id, worker_id="w1", on_deferral=lambda *_: None)


def test_a_relay_that_never_deferred_sends_what_it_always_sent(lanes: None) -> None:
    """A single str lane, with no deferral: the same statements as before lanes."""
    _fire(Sms, 3)
    with _statements() as relayed:
        run_relay(worker_id="w1", passes=1, lane="sms", now=_Clock(), **UNSAFE)

    _fire(Sms, 3)
    with _statements() as before:
        _the_pass_without_lanes(lane="sms", clock=_Clock())

    assert relayed == before
    assert not [sql for sql in relayed if "lanepauserecord" in sql]


def test_with_no_named_lanes_every_lane_sends_what_it_always_sent(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """lane=None in a lane-less project claims exactly as None always did."""
    assert registry.lanes() == ["default"]
    for _ in range(2):
        with transaction.atomic():
            fire(order)
    with _claims(monkeypatch) as made, _statements() as relayed:
        run_relay(worker_id="w1", passes=1, lane=None, now=_Clock(), **UNSAFE)

    for _ in range(2):
        with transaction.atomic():
            fire(order)
    with _statements() as before:
        _the_pass_without_lanes(lane=None, clock=_Clock())

    assert relayed == before
    assert [claim["lease"] for claim in made] == [
        timedelta(seconds=registry.claim_lease_seconds(None, default=setting("LEASE_SECONDS")))
    ]
