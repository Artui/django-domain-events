from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timedelta

import pytest
from django.contrib.auth import get_user_model
from django.db import DEFAULT_DB_ALIAS, OperationalError, connection, connections, transaction
from django.db.backends.postgresql import psycopg_any
from django.utils import timezone as django_timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.wake import wait_for_work
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.settings import setting
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered, receiver_replaced
from tests.testapp.events import OrderPlaced, calls

pytestmark = pytest.mark.django_db(transaction=True)

# The loop is backend-agnostic; only the concurrency guard is not. Separating
# them is what keeps the whole relay testable on the default backend, the same
# way the clock and the sleep are arguments.
UNSAFE = {"allow_unsafe_concurrency": True}


def test_it_refuses_where_locks_cannot_be_skipped() -> None:
    """Without skip-locked, two relays hand the same row to two receivers."""
    if connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend supports skipped locks")
    with pytest.raises(RuntimeError, match="SKIP LOCKED"):
        run_relay(worker_id="w1", passes=1)


def test_it_runs_where_the_backend_supports_skipped_locks(
    order: OrderPlaced, record: list[str]
) -> None:
    if not connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend cannot skip locks")
    with transaction.atomic():
        fire(order)
    record.clear()
    assert run_relay(worker_id="w1", passes=1) == {DeliveryStatus.SUCCEEDED: 2}


def test_it_claims_and_delivers(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    record.clear()

    assert run_relay(worker_id="w1", passes=1, **UNSAFE) == {DeliveryStatus.SUCCEEDED: 2}
    assert not DeliveryRecord.objects.exclude(status=DeliveryStatus.SUCCEEDED).exists()


def test_an_idle_pass_waits_rather_than_spinning() -> None:
    """The idle wait is an argument so the branch is reachable without elapsing real seconds."""
    waited: list[float] = []
    run_relay(worker_id="w1", passes=2, wait=lambda t: bool(waited.append(t)), **UNSAFE)
    assert len(waited) == 2


def test_a_pass_with_work_does_not_wait(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    waited: list[float] = []
    run_relay(worker_id="w1", passes=1, wait=lambda t: bool(waited.append(t)), **UNSAFE)
    assert waited == []


def test_the_clock_is_an_argument(order: OrderPlaced, record: list[str]) -> None:
    """A row serving a backoff wait is invisible until its time, by clock move."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(available_at=django_timezone.now() + timedelta(hours=1))

    assert run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE) == {}

    later = lambda: django_timezone.now() + timedelta(hours=2)  # noqa: E731
    assert run_relay(worker_id="w1", passes=1, now=later, wait=lambda _: False, **UNSAFE) == {
        DeliveryStatus.SUCCEEDED: 2
    }


def test_the_isolation_helper_swallows_what_deliver_one_does_not() -> None:
    """deliver_one guards the receiver and the decode, not the row fetch."""
    from django_domain_events.delivery.run_relay import _deliver_or_survive

    assert _deliver_or_survive(999_999, "w1", connections[write_alias()]) is None


def test_a_lost_row_does_not_stop_the_pass(order: OrderPlaced, record: list[str]) -> None:
    """Rows the relay lost are skipped, and must not end the batch."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    def steal_everything_else(evt: OrderPlaced) -> None:
        DeliveryRecord.objects.exclude(receiver_key="testapp.durable_receiver").update(
            claimed_by="someone-else", claimed_at=django_timezone.now()
        )

    with transaction.atomic():
        fire(order)
        fire(order)

    with receiver_replaced("testapp.durable_receiver", steal_everything_else):
        counts = run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE)

    lost = DeliveryRecord.objects.filter(claimed_by="someone-else").count()
    assert lost >= 2, "nothing was lost mid-pass, so the loop was never resumed"
    assert counts == {DeliveryStatus.SUCCEEDED: 2}


def test_a_failure_while_idling_does_not_kill_the_daemon() -> None:
    """The relay spends nearly all its life waiting."""
    calls: list[float] = []

    def explode(timeout: float) -> bool:
        calls.append(timeout)
        raise RuntimeError("the database went away mid-wait")

    counts = run_relay(worker_id="w1", passes=2, sleep=lambda _: None, wait=explode, **UNSAFE)

    assert counts == {}
    assert len(calls) == 2, "the relay stopped at the first failed wait"


def test_a_wait_that_fails_at_once_still_idles_for_the_poll(settings) -> None:
    """A wait that raises returns at once, so an idle relay does not spin."""
    settings.DJANGO_DOMAIN_EVENTS = {"POLL_SECONDS": 0.25}
    slept: list[float] = []

    def explode(timeout: float) -> bool:
        raise TypeError("'list' object is not callable")

    run_relay(worker_id="w1", passes=2, sleep=slept.append, wait=explode, **UNSAFE)

    assert slept == [0.25, 0.25], "a failed wait was followed by an immediate claim"


# --- surviving the database ---------------------------------------------------
#
# The close is spied on rather than observed: the SQLite test database lives in
# memory, and Django ignores close() there because closing would destroy it. The
# Postgres test further down shows the close doing its job on a real server.


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


def _claims_failing(monkeypatch: pytest.MonkeyPatch, outcomes: list[bool]) -> None:
    """Make each claim fail or succeed in turn: ``False`` raises, ``True`` claims."""
    real = run_relay_module.claim_batch
    queue = list(outcomes)

    def claim(**kwargs: object) -> list[int]:
        if not queue.pop(0):
            raise OperationalError("server closed the connection unexpectedly")
        return real(**kwargs)

    monkeypatch.setattr(run_relay_module, "claim_batch", claim)


def test_a_failed_claim_does_not_kill_the_relay(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch, closes: list[str]
) -> None:
    """The claim was the one unguarded call in the loop, so one error ended the daemon."""
    with transaction.atomic():
        fire(order)
    _claims_failing(monkeypatch, [False, True])
    slept: list[float] = []

    counts = run_relay(worker_id="w1", passes=2, sleep=slept.append, wait=lambda _: False, **UNSAFE)

    assert counts == {DeliveryStatus.SUCCEEDED: 2}
    assert closes == [write_alias()], "the connection was not closed after the failed claim"
    assert slept == [1.0], "the relay retried without backing off"


def test_a_raw_driver_error_while_idling_closes_the_connection(closes: list[str]) -> None:
    """The wait reaches past Django's cursor, so a drop raises a driver exception."""
    conn = connections[write_alias()]

    def drop(timeout: float) -> bool:
        raise conn.Database.OperationalError("the connection is lost")

    run_relay(worker_id="w1", passes=1, sleep=lambda _: None, wait=drop, **UNSAFE)

    assert closes == [write_alias()]


def test_a_database_error_in_a_delivery_closes_the_connection(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch, closes: list[str]
) -> None:
    """A delivery is where a gone database is most likely found mid-pass."""
    with transaction.atomic():
        fire(order)

    def gone(delivery_id: int, *, worker_id: str, **_: object) -> None:
        raise OperationalError("terminating connection due to administrator command")

    monkeypatch.setattr(run_relay_module, "dispatch_one", gone)
    run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE)

    assert closes == [write_alias(), write_alias()]


def test_an_error_that_is_not_the_databases_keeps_the_connection(closes: list[str]) -> None:
    """Closing costs a reconnect, so a per-row misconfiguration pays it per row."""

    def explode(timeout: float) -> bool:
        raise RuntimeError("not a database problem")

    run_relay(worker_id="w1", passes=1, sleep=lambda _: None, wait=explode, **UNSAFE)

    assert closes == []


def test_a_close_that_raises_does_not_kill_the_relay(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Django discards the connection even when closing raises, so say so and carry on."""

    def refuse() -> None:
        raise OperationalError("could not close")

    monkeypatch.setattr(connections[write_alias()], "close", refuse)
    _claims_failing(monkeypatch, [False, True])

    counts = run_relay(
        worker_id="w1", passes=2, sleep=lambda _: None, wait=lambda _: False, **UNSAFE
    )

    assert counts == {}
    assert "could not close its connection" in caplog.text


def test_repeated_claim_failures_back_off_to_a_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Doubling from the poll interval, capped inside a default grace period."""
    _claims_failing(monkeypatch, [False] * 7)
    slept: list[float] = []

    run_relay(worker_id="w1", passes=7, sleep=slept.append, **UNSAFE)

    assert slept == [1.0, 2.0, 4.0, 8.0, 15.0, 15.0, 15.0]


def test_a_claim_that_succeeds_resets_the_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """An outage is measured from its own first failure."""
    _claims_failing(monkeypatch, [False, False, True, False])
    slept: list[float] = []

    run_relay(worker_id="w1", passes=4, sleep=slept.append, wait=lambda _: False, **UNSAFE)

    assert slept == [1.0, 2.0, 1.0]


def test_a_connection_killed_while_idling_is_replaced(
    order: OrderPlaced, record: list[str]
) -> None:
    """The real thing: the backend is terminated while the relay waits for work."""
    if connection.vendor != "postgresql":
        pytest.skip("needs a server whose backend can be terminated")
    if not psycopg_any.is_psycopg3:
        pytest.skip(
            "psycopg2 cannot listen, so its wait sleeps without touching the "
            "connection and the claim's transaction finds the death instead"
        )
    with transaction.atomic():
        fire(order)
    record.clear()
    DeliveryRecord.objects.update(available_at=django_timezone.now() + timedelta(hours=1))
    waited: list[float] = []

    def clock() -> datetime:
        return django_timezone.now() + (timedelta(hours=2) if waited else timedelta(0))

    def kill_then_wait(timeout: float) -> bool:
        waited.append(timeout)
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_backend_pid()")
            (pid,) = cursor.fetchone()
        other = connections.create_connection(DEFAULT_DB_ALIAS)
        try:
            with other.cursor() as cursor:
                cursor.execute("SELECT pg_terminate_backend(%s)", [pid])
        finally:
            other.close()
        return wait_for_work(timeout)

    try:
        counts = run_relay(
            worker_id="w1", passes=2, now=clock, sleep=lambda _: None, wait=kill_then_wait
        )
    finally:
        connection.close()

    assert waited, "the first pass did not idle, so nothing was killed"
    assert counts == {DeliveryStatus.SUCCEEDED: 2}


# --- stopping -----------------------------------------------------------------


class _Runaway(BaseException):
    """Raised by a test double called past what a working stop allows."""


def test_a_stop_before_the_claim_claims_nothing(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)

    assert run_relay(worker_id="w1", stop=lambda: True, wait=lambda _: False, **UNSAFE) == {}
    assert not DeliveryRecord.objects.exclude(status=DeliveryStatus.PENDING).exists()


def test_a_stop_mid_batch_finishes_the_row_in_hand_and_hands_back_the_rest(
    order: OrderPlaced, record: list[str]
) -> None:
    """The rest of the batch is claimable at once, not when its lease lapses."""
    with transaction.atomic():
        fire(order)
        fire(order)
    record.clear()

    counts = run_relay(worker_id="w1", stop=lambda: bool(record), wait=lambda _: False, **UNSAFE)

    assert counts == {DeliveryStatus.SUCCEEDED: 1}, "the row in hand was not finished"
    handed_back = DeliveryRecord.objects.filter(status=DeliveryStatus.CLAIMED)
    assert handed_back.count() == 3
    taken = claim_batch(
        worker_id="w2", now=django_timezone.now(), lease=timedelta(minutes=5), limit=10
    )
    assert sorted(taken) == sorted(handed_back.values_list("pk", flat=True))


def test_a_stop_while_idling_ends_the_relay() -> None:
    """Without passes the relay runs forever: the stop must be read after the wait."""
    waited: list[float] = []

    def wait(timeout: float) -> bool:
        waited.append(timeout)
        if len(waited) > 1:
            raise _Runaway("the stop was not read after the wait")
        return False

    run_relay(worker_id="w1", stop=lambda: bool(waited), wait=wait, **UNSAFE)

    assert len(waited) == 1


class _Recorder:
    """A one-method task backend that keeps the ids it was handed."""

    enqueued: list[int] = []

    def enqueue(self, delivery_id: int, **claim: object) -> None:
        self.enqueued.append(delivery_id)


def test_a_row_handed_to_the_task_queue_is_not_handed_back(
    order: OrderPlaced, record: list[str], settings
) -> None:
    """An enqueued row stays CLAIMED under the relay's claim until the task takes it."""
    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": f"{__name__}._Recorder"}
    _Recorder.enqueued = []
    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "site", "task")
    with transaction.atomic():
        fire(order)
    try:
        run_relay(
            worker_id="w1",
            passes=2,
            stop=lambda: bool(_Recorder.enqueued),
            wait=lambda _: False,
            **UNSAFE,
        )
    finally:
        object.__setattr__(entry, "site", "relay")

    now = django_timezone.now()
    enqueued = DeliveryRecord.objects.get(receiver_key="testapp.durable_receiver")
    rest = DeliveryRecord.objects.exclude(pk=enqueued.pk).get()
    assert _Recorder.enqueued == [enqueued.pk], "the task row was not first in the batch"
    assert rest.lease_expires_at < now, "the row after the stop was not handed back"
    assert enqueued.lease_expires_at > now, "the enqueued row's lease was expired"


def test_a_hand_back_that_fails_does_not_kill_the_relay(
    order: OrderPlaced,
    record: list[str],
    monkeypatch: pytest.MonkeyPatch,
    closes: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Rows the exiting process could not give back are reclaimed when they lapse."""
    with transaction.atomic():
        fire(order)
    record.clear()

    def broken(*args: object, **kwargs: object) -> int:
        raise OperationalError("the connection is lost")

    monkeypatch.setattr(run_relay_module, "hand_back", broken)
    counts = run_relay(worker_id="w1", stop=lambda: bool(record), wait=lambda _: False, **UNSAFE)

    assert counts == {DeliveryStatus.SUCCEEDED: 1}
    assert "could not hand back" in caplog.text
    assert closes == [write_alias()]


def test_an_exit_raised_mid_delivery_ends_the_relay_and_rolls_the_receiver_back(
    order: OrderPlaced, record: list[str]
) -> None:
    """A second signal makes deliver_events raise SystemExit from its handler."""
    with transaction.atomic():
        fire(order)
    record.clear()
    users = get_user_model().objects

    def interrupted(evt: OrderPlaced) -> None:
        users.create(username="written-before-the-exit")
        record.append("interrupted")
        raise SystemExit(128 + 15)

    with receiver_replaced("testapp.durable_receiver", interrupted), pytest.raises(SystemExit):
        run_relay(worker_id="w1", passes=2, wait=lambda _: False, **UNSAFE)

    assert record[-1] == "interrupted", "a delivery ran after the exit"
    assert not users.filter(username="written-before-the-exit").exists()
    interrupted_row = DeliveryRecord.objects.get(receiver_key="testapp.durable_receiver")
    assert interrupted_row.status == DeliveryStatus.CLAIMED
    assert interrupted_row.lease_expires_at > django_timezone.now()


def test_an_exit_mid_batch_hands_nothing_back(order: OrderPlaced, record: list[str]) -> None:
    """A second signal leaves the unstarted rest of the batch to lapse under its lease."""
    with transaction.atomic():
        fire(order)
    exits: list[str] = []

    def first_call_exits(*args: object) -> None:
        if not exits:
            exits.append("exit")
            raise SystemExit(128 + 15)

    with (
        receiver_replaced("testapp.durable_receiver", first_call_exits),
        receiver_replaced("testapp.with_context", first_call_exits),
        pytest.raises(SystemExit),
    ):
        run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE)

    rows = DeliveryRecord.objects.filter(
        receiver_key__in=["testapp.durable_receiver", "testapp.with_context"]
    )
    assert rows.count() == 2
    assert {(row.status, row.claimed_by) for row in rows} == {(DeliveryStatus.CLAIMED, "w1")}
    assert len({row.claimed_at for row in rows}) == 1
    assert all(row.lease_expires_at > django_timezone.now() for row in rows)


def test_a_relay_on_an_injected_clock_spends_an_attempt_on_a_lapse(
    order: OrderPlaced, record: list[str]
) -> None:
    """Only the test helper is exempt: a relay told the time still counts the lapse."""
    seen: list[DeliveryFailure] = []
    entry = dataclasses.replace(_mail(), max_attempts=1, on_failure=seen.append)
    later = django_timezone.now() + timedelta(days=2)
    with receiver_registered(entry):
        with transaction.atomic():
            fire(order)
        claim_batch(
            worker_id="dead", now=django_timezone.now(), lease=timedelta(seconds=1), limit=10
        )

        run_relay(
            worker_id="w1", passes=1, now=lambda: later, lane="mail", wait=lambda _: False, **UNSAFE
        )

    row = DeliveryRecord.objects.get(receiver_key="tests.mail")
    assert (row.status, row.attempts) == (DeliveryStatus.DEAD, 1)
    assert [(f.status, f.attempt) for f in seen] == [(DeliveryStatus.DEAD, 1)]


def _mail() -> RegisteredReceiver:
    return RegisteredReceiver(
        key="tests.mail",
        event_class=OrderPlaced,
        func=lambda evt: calls.append("mail"),
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane="mail",
    )


def test_a_relay_serves_the_default_lane_unless_told_otherwise(
    order: OrderPlaced, record: list[str]
) -> None:
    """The default relay staying out of a named lane is what isolation means."""
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        record.clear()

        assert run_relay(worker_id="w1", passes=1, **UNSAFE) == {DeliveryStatus.SUCCEEDED: 2}
        assert "mail" not in record
        assert run_relay(worker_id="w2", passes=1, lane="mail", **UNSAFE) == {
            DeliveryStatus.SUCCEEDED: 1
        }
        assert record[-1] == "mail"


def test_a_relay_given_no_lane_at_all_serves_every_lane(
    order: OrderPlaced, record: list[str]
) -> None:
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)

        assert run_relay(worker_id="w1", passes=1, lane=None, **UNSAFE) == {
            DeliveryStatus.SUCCEEDED: 3
        }


def test_a_relay_for_a_lane_nobody_declared_refuses_to_start() -> None:
    """It would claim nothing, forever, and look healthy doing it."""
    with pytest.raises(ValueError, match="No receiver is declared in lane 'mial'"):
        run_relay(worker_id="w1", passes=1, lane="mial", **UNSAFE)


def test_a_relay_claims_in_batches_of_its_own_size(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per process, for claims only: a mail relay needs a batch that fits one lease."""
    limits: list[int] = []
    real = run_relay_module.claim_batch

    def spy(**kwargs: object) -> list[int]:
        limits.append(kwargs["limit"])
        return real(**kwargs)

    monkeypatch.setattr(run_relay_module, "claim_batch", spy)
    with transaction.atomic():
        fire(order)

    run_relay(worker_id="w1", passes=1, batch_size=1, **UNSAFE)
    run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE)

    assert limits == [1, setting("BATCH_SIZE")]


@pytest.mark.parametrize("size", [0, -1])
def test_a_relay_refuses_a_batch_that_claims_nothing(size: int) -> None:
    """A batch of zero claims nothing on every pass: a relay that idles forever with work owed."""
    with pytest.raises(ValueError, match="batch_size must be positive"):
        run_relay(worker_id="w1", passes=1, batch_size=size, **UNSAFE)


# --- A deferral pauses its lane ----------------------------------------------


def _throttled(seen: list[int]) -> RegisteredReceiver:
    """A mail-lane receiver whose destination throttles: each call defers 30 s."""

    def throttled(evt: OrderPlaced) -> None:
        seen.append(evt.order_id)
        if len(seen) > 10:
            raise _Runaway("the paused lane kept being attempted")
        raise RetryAfter(30, reason="throttled", counts=False)

    return RegisteredReceiver(
        key="tests.throttled",
        event_class=OrderPlaced,
        func=throttled,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane="mail",
        give_up_after=timedelta(days=1),
    )


class _Clock:
    """An injected clock that idling moves on, ten seconds per wait."""

    def __init__(self) -> None:
        self.at = django_timezone.now()

    def __call__(self) -> datetime:
        return self.at

    def wait(self, timeout: float) -> bool:
        self.at += timedelta(seconds=10)
        return False


def _given_back(key: str) -> list[int]:
    return sorted(
        DeliveryRecord.objects.filter(
            receiver_key=key,
            status=DeliveryStatus.CLAIMED,
            lease_expires_at__lt=django_timezone.now(),
        ).values_list("pk", flat=True)
    )


def _fire_three(order: OrderPlaced) -> None:
    for _ in range(3):
        with transaction.atomic():
            fire(order)


def test_a_deferral_pauses_the_lane_and_hands_back_the_rest_of_the_batch(
    order: OrderPlaced, record: list[str]
) -> None:
    """The rest of the batch is not attempted, and any worker can claim it at once."""
    seen: list[int] = []
    with receiver_registered(_throttled(seen)):
        _fire_three(order)
        clock = _Clock()

        counts = run_relay(
            worker_id="w1", passes=3, lane="mail", now=clock, wait=clock.wait, **UNSAFE
        )

        assert counts == {DeliveryStatus.FAILED: 1}
        assert len(seen) == 1
        handed_back = _given_back("tests.throttled")
        assert len(handed_back) == 2
        taken = claim_batch(
            worker_id="w2",
            now=django_timezone.now(),
            lease=timedelta(minutes=5),
            limit=10,
            lane="mail",
        )
        assert sorted(taken) == handed_back


@pytest.mark.parametrize(("passes", "attempted"), [(4, 1), (5, 2)])
def test_the_pause_ends_when_the_requested_delay_has_passed_on_the_injected_clock(
    order: OrderPlaced, record: list[str], passes: int, attempted: int
) -> None:
    """Thirty seconds asked, ten per idle wait: passes two to four fall inside."""
    seen: list[int] = []
    with receiver_registered(_throttled(seen)):
        _fire_three(order)
        clock = _Clock()

        run_relay(worker_id="w1", passes=passes, lane="mail", now=clock, wait=clock.wait, **UNSAFE)

    assert len(seen) == attempted


def _search() -> RegisteredReceiver:
    """A receiver in a lane that sorts after mail, reached after mail's deferral."""
    return dataclasses.replace(
        _mail(), key="tests.search", func=lambda evt: calls.append("search"), lane="search"
    )


def test_a_deferral_in_one_lane_leaves_the_next_lanes_claim_in_the_same_pass(
    order: OrderPlaced, record: list[str]
) -> None:
    """Only the deferring lane's batch goes back; the pass goes on to the next lane."""
    seen: list[int] = []
    with receiver_registered(_throttled(seen)), receiver_registered(_search()):
        _fire_three(order)
        record.clear()
        clock = _Clock()

        counts = run_relay(
            worker_id="w1", passes=1, lane=None, now=clock, wait=clock.wait, **UNSAFE
        )

        assert counts == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 9}
        assert len(seen) == 1
        assert len(_given_back("tests.throttled")) == 2
        assert record.count("search") == 3


def test_a_relay_serving_every_lane_keeps_claiming_the_other_lanes_while_one_is_paused(
    order: OrderPlaced, record: list[str]
) -> None:
    """Batches of three: the second pass claims default rows and leaves mail alone."""
    seen: list[int] = []
    with receiver_registered(_throttled(seen)):
        _fire_three(order)
        clock = _Clock()

        counts = run_relay(
            worker_id="w1",
            passes=4,
            lane=None,
            batch_size=3,
            now=clock,
            wait=clock.wait,
            **UNSAFE,
        )

        assert counts == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 6}
        assert len(seen) == 1
        assert len(_given_back("tests.throttled")) == 2, "the paused lane was claimed again"


def test_a_stop_after_a_deferral_hands_back_only_what_is_still_held(
    order: OrderPlaced, record: list[str], caplog: pytest.LogCaptureFixture
) -> None:
    """The deferral already gave ``mail``'s rows back."""
    seen: list[int] = []
    with receiver_registered(_throttled(seen)), receiver_registered(_search()):
        _fire_three(order)
        record.clear()
        clock = _Clock()

        with caplog.at_level(logging.INFO, logger="django_domain_events.delivery.run_relay"):
            run_relay(
                worker_id="w1",
                lane=["mail", "search"],
                stop=lambda: record.count("search") >= 2,
                now=clock,
                wait=clock.wait,
                **UNSAFE,
            )

    assert "paused lane mail after a deferral; handed back 2 unstarted rows" in caplog.text
    assert "stopping; handed back 1 unstarted rows" in caplog.text


def test_a_relay_whose_lane_is_paused_does_not_query_it(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skipping it spares a query per poll that would walk past every owed row."""
    claims: list[dict[str, object]] = []
    real = run_relay_module.claim_batch

    def spy(**kwargs: object) -> list[int]:
        claims.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(run_relay_module, "claim_batch", spy)
    with receiver_registered(_throttled([])):
        _fire_three(order)
        clock = _Clock()

        run_relay(worker_id="w1", passes=3, lane="mail", now=clock, wait=clock.wait, **UNSAFE)

    assert len(claims) == 1, "a relay claimed from the lane it had paused"


def test_a_set_aside_that_fails_abandons_the_batch(
    order: OrderPlaced,
    record: list[str],
    monkeypatch: pytest.MonkeyPatch,
    closes: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """After the failure the rest of the paused batch waits out its lease."""
    seen: list[int] = []

    def broken(*args: object, **kwargs: object) -> int:
        raise OperationalError("the connection is lost")

    with receiver_registered(_throttled(seen)):
        _fire_three(order)
        monkeypatch.setattr(run_relay_module, "hand_back", broken)
        clock = _Clock()

        counts = run_relay(
            worker_id="w1", passes=3, lane="mail", now=clock, wait=clock.wait, **UNSAFE
        )

        assert counts == {DeliveryStatus.FAILED: 1}
        assert len(seen) == 1
        abandoned = DeliveryRecord.objects.filter(
            status=DeliveryStatus.CLAIMED, lease_expires_at__gt=django_timezone.now()
        )
        assert abandoned.count() == 2, "the rest of the batch was not left to its lease"
    assert "could not hand back the rows of paused lane mail" in caplog.text
    assert closes == [write_alias()]
