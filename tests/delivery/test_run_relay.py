from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.contrib.auth import get_user_model
from django.db import DEFAULT_DB_ALIAS, OperationalError, connection, connections, transaction

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.wake import wait_for_work
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

# The loop is backend-agnostic; only the concurrency guard is not. Separating
# them is what keeps the whole relay testable on the default backend, the same
# way the clock and the sleep are arguments.
UNSAFE = {"allow_unsafe_concurrency": True}


def test_it_refuses_where_locks_cannot_be_skipped() -> None:
    """Two relays on a backend without skipped locks would hand the same row to
    two receivers on every pass."""
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
    """The idle wait is an argument so the branch is reachable without elapsing
    real seconds. ``wait`` rather than ``sleep``: on a backend that can notify,
    the loop blocks on the notification instead of sleeping, so injecting the
    sleep would control the loop on SQLite and not on Postgres."""
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
    """A row serving a backoff wait is invisible until its time comes, and that
    is asserted by moving the clock rather than by waiting."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(available_at=datetime.now(timezone.utc) + timedelta(hours=1))

    assert run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE) == {}

    later = lambda: datetime.now(timezone.utc) + timedelta(hours=2)  # noqa: E731
    assert run_relay(worker_id="w1", passes=1, now=later, wait=lambda _: False, **UNSAFE) == {
        DeliveryStatus.SUCCEEDED: 2
    }


def test_the_isolation_helper_swallows_what_deliver_one_does_not() -> None:
    """deliver_one guards the receiver and the decode, not the row fetch. A row
    that vanished between the claim and the delivery must not take the daemon
    with it."""
    from django_domain_events.delivery.run_relay import _deliver_or_survive

    assert _deliver_or_survive(999_999, "w1", connections[write_alias()]) is None


def test_a_lost_row_does_not_stop_the_pass(order: OrderPlaced, record: list[str]) -> None:
    """The relay counts outcomes and skips rows it lost, which must not end the
    batch: two events so a lost row lands mid-pass rather than at the end."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    def steal_everything_else(evt: OrderPlaced) -> None:
        DeliveryRecord.objects.exclude(receiver_key="testapp.durable_receiver").update(
            claimed_by="someone-else", claimed_at=datetime.now(timezone.utc)
        )

    with transaction.atomic():
        fire(order)
        fire(order)

    with receiver_replaced("testapp.durable_receiver", steal_everything_else):
        counts = run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE)

    # Deterministic regardless of how many receivers are registered: the first
    # row delivered succeeds and takes every other row away from this worker, so
    # the rest are lost mid-pass rather than at its end.
    lost = DeliveryRecord.objects.filter(claimed_by="someone-else").count()
    assert lost >= 2, "nothing was lost mid-pass, so the loop was never resumed"
    assert counts == {DeliveryStatus.SUCCEEDED: 2}


def test_a_failure_while_idling_does_not_kill_the_daemon() -> None:
    """The relay spends nearly all its life waiting. The wait reaches past
    Django's cursor to the driver, so a connection dropped there raises the
    driver's own exception rather than a translated django.db.Error - which a
    supervisor written to catch the latter would miss entirely."""
    calls: list[float] = []

    def explode(timeout: float) -> bool:
        calls.append(timeout)
        raise RuntimeError("the database went away mid-wait")

    counts = run_relay(worker_id="w1", passes=2, wait=explode, **UNSAFE)

    assert counts == {}
    assert len(calls) == 2, "the relay stopped at the first failed wait"


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
    """The claim was the one unguarded call in the loop, so one database error
    there ended the daemon. Guarding it is not enough on its own: Django only
    reconnects a connection that has been set to None, so the relay closes the
    connection before it tries again."""
    with transaction.atomic():
        fire(order)
    _claims_failing(monkeypatch, [False, True])
    slept: list[float] = []

    counts = run_relay(worker_id="w1", passes=2, sleep=slept.append, wait=lambda _: False, **UNSAFE)

    assert counts == {DeliveryStatus.SUCCEEDED: 2}
    assert closes == [write_alias()], "the connection was not closed after the failed claim"
    assert slept == [1.0], "the relay retried without backing off"


def test_a_raw_driver_error_while_idling_closes_the_connection(closes: list[str]) -> None:
    """The wait reaches past Django's cursor, so a dropped connection there can
    surface as the driver's own exception, which is not a ``django.db.Error``.
    Holds the driver half of what counts as a database error."""
    conn = connections[write_alias()]

    def drop(timeout: float) -> bool:
        raise conn.Database.OperationalError("the connection is lost")

    run_relay(worker_id="w1", passes=1, wait=drop, **UNSAFE)

    assert closes == [write_alias()]


def test_a_database_error_in_a_delivery_closes_the_connection(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch, closes: list[str]
) -> None:
    """A delivery is where the database is most likely to be found gone, mid
    pass, and the batch goes on to its next row on the same connection."""
    with transaction.atomic():
        fire(order)

    def gone(delivery_id: int, *, worker_id: str) -> None:
        raise OperationalError("terminating connection due to administrator command")

    monkeypatch.setattr(run_relay_module, "dispatch_one", gone)
    run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE)

    assert closes == [write_alias(), write_alias()]


def test_an_error_that_is_not_the_databases_keeps_the_connection(closes: list[str]) -> None:
    """Closing costs a reconnect, and a misconfiguration raising once per row
    would pay it once per row. Only a database error says the connection may be
    gone."""

    def explode(timeout: float) -> bool:
        raise RuntimeError("not a database problem")

    run_relay(worker_id="w1", passes=1, wait=explode, **UNSAFE)

    assert closes == []


def test_a_close_that_raises_does_not_kill_the_relay(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Django discards the connection object even when closing it raises, so
    there is nothing left to do but say so and carry on."""

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
    """Doubling from the poll interval, and capped well inside a default
    Kubernetes grace period: the sleep is not interrupted by a stop request, so
    the cap is also the longest a database outage can delay a shutdown."""
    _claims_failing(monkeypatch, [False] * 7)
    slept: list[float] = []

    run_relay(worker_id="w1", passes=7, sleep=slept.append, **UNSAFE)

    assert slept == [1.0, 2.0, 4.0, 8.0, 15.0, 15.0, 15.0]


def test_a_claim_that_succeeds_resets_the_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """An outage is measured from its own first failure, not from the first
    failure the process ever saw."""
    _claims_failing(monkeypatch, [False, False, True, False])
    slept: list[float] = []

    run_relay(worker_id="w1", passes=4, sleep=slept.append, wait=lambda _: False, **UNSAFE)

    assert slept == [1.0, 2.0, 1.0]


def test_a_connection_killed_while_idling_is_replaced(
    order: OrderPlaced, record: list[str]
) -> None:
    """The real thing, on a real server: the backend is terminated while the
    relay waits for work, which is where it spends its life.

    Where the death surfaces is what decides whether the close is needed.
    Found first inside the claim's transaction, Django closes the connection
    itself when the rollback fails, and the next claim reconnects. Found first
    in autocommit, as the wait's LISTEN finds it, nothing closes it, and every
    later claim fails with "the connection is lost" for as long as the process
    lives - which, with the claim guarded, is forever.
    """
    if connection.vendor != "postgresql":
        pytest.skip("needs a server whose backend can be terminated")
    with transaction.atomic():
        fire(order)
    record.clear()
    # Not owed until the clock moves, so the first pass idles and the second,
    # after the kill, has something to deliver.
    DeliveryRecord.objects.update(available_at=datetime.now(timezone.utc) + timedelta(hours=1))
    waited: list[float] = []

    def clock() -> datetime:
        return datetime.now(timezone.utc) + (timedelta(hours=2) if waited else timedelta(0))

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
        # So the suite's own teardown has a live connection whatever happened.
        connection.close()

    assert waited, "the first pass did not idle, so nothing was killed"
    assert counts == {DeliveryStatus.SUCCEEDED: 2}


# --- stopping -----------------------------------------------------------------


class _Runaway(BaseException):
    """Raised by a test double that has been called more often than a working
    stop allows, so a broken stop fails the test instead of hanging the suite.
    A BaseException because the relay swallows every ``Exception`` its wait
    raises."""


def test_a_stop_before_the_claim_claims_nothing(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)

    assert run_relay(worker_id="w1", stop=lambda: True, wait=lambda _: False, **UNSAFE) == {}
    assert not DeliveryRecord.objects.exclude(status=DeliveryStatus.PENDING).exists()


def test_a_stop_mid_batch_finishes_the_row_in_hand_and_hands_back_the_rest(
    order: OrderPlaced, record: list[str]
) -> None:
    """The rest of the batch is claimable by another worker at once, rather than
    when the lease the claim stamped on it lapses - minutes, by default."""
    with transaction.atomic():
        fire(order)
        fire(order)
    record.clear()

    counts = run_relay(worker_id="w1", stop=lambda: bool(record), wait=lambda _: False, **UNSAFE)

    assert counts == {DeliveryStatus.SUCCEEDED: 1}, "the row in hand was not finished"
    handed_back = DeliveryRecord.objects.filter(status=DeliveryStatus.CLAIMED)
    assert handed_back.count() == 3
    taken = claim_batch(
        worker_id="w2", now=datetime.now(timezone.utc), lease=timedelta(minutes=5), limit=10
    )
    assert sorted(taken) == sorted(handed_back.values_list("pk", flat=True))


def test_a_stop_while_idling_ends_the_relay() -> None:
    """Without ``passes`` the relay runs forever, so this returns only if the
    stop is read after the wait."""
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
    """An enqueued row is still CLAIMED under the relay's claim, waiting for the
    task to take it, so the claim alone cannot tell it from an unstarted row.
    Expiring its lease would have another relay enqueue it a second time."""
    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": f"{__name__}._Recorder"}
    _Recorder.enqueued = []
    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "site", "task")
    with transaction.atomic():
        fire(order)
    try:
        # Bounded, so a batch in an unexpected order ends rather than relaying
        # forever with nothing to stop it.
        run_relay(
            worker_id="w1",
            passes=2,
            stop=lambda: bool(_Recorder.enqueued),
            wait=lambda _: False,
            **UNSAFE,
        )
    finally:
        object.__setattr__(entry, "site", "relay")

    now = datetime.now(timezone.utc)
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
    """The process is on its way out, and the rows it could not give back are
    reclaimed when their lease lapses, as a crashed worker's are."""
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
    """What a second signal does: ``deliver_events`` raises ``SystemExit`` from
    its handler, wherever the main thread is. The relay swallows every
    ``Exception`` a delivery raises, so this holds that its guards stay that
    narrow - widened to ``BaseException``, they would swallow the exit and the
    relay would carry on. The receiver's work rolls back with its transaction,
    and its row stays claimed for its lease to lapse, as a crashed worker's
    does."""
    with transaction.atomic():
        fire(order)
    record.clear()
    users = get_user_model().objects

    def interrupted(evt: OrderPlaced) -> None:
        users.create(username="written-before-the-exit")
        record.append("interrupted")
        raise SystemExit(128 + 15)

    # Bounded, so an exit that is swallowed fails the raises below rather than
    # leaving the relay running forever.
    with receiver_replaced("testapp.durable_receiver", interrupted), pytest.raises(SystemExit):
        run_relay(worker_id="w1", passes=2, wait=lambda _: False, **UNSAFE)

    assert record[-1] == "interrupted", "a delivery ran after the exit"
    assert not users.filter(username="written-before-the-exit").exists()
    interrupted_row = DeliveryRecord.objects.get(receiver_key="testapp.durable_receiver")
    assert interrupted_row.status == DeliveryStatus.CLAIMED
    assert interrupted_row.lease_expires_at > datetime.now(timezone.utc)
