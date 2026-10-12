from __future__ import annotations

import importlib
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import mock

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone as django_timezone

from django_domain_events import cancel_deliveries
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.prune_events import prune_events
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import stored
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

DURABLE = "testapp.durable_receiver"
CONTEXT = "testapp.with_context"


def _fired(order: OrderPlaced) -> int:
    with transaction.atomic():
        return fire(order)


def _status(event_id: int, key: str) -> str:
    return DeliveryRecord.objects.values_list("status", flat=True).get(
        event=event_id, receiver_key=key
    )


def _statuses(event_id: int) -> dict[str, str]:
    return dict(DeliveryRecord.objects.filter(event=event_id).values_list("receiver_key", "status"))


def test_unclaimed_rows_are_settled_and_never_run(order: OrderPlaced, record: list[str]) -> None:
    """The point of the operation: work withdrawn before it ran never runs,
    not now and not when the relay is told a time far beyond its due time."""
    event_id = _fired(order)
    record.clear()

    assert cancel_deliveries([event_id]) == {"cancelled": 2, "in_flight": 0}

    assert set(_statuses(event_id).values()) == {DeliveryStatus.CANCELLED}
    assert drain_outbox() == {}
    assert drain_outbox(now=django_timezone.now() + timedelta(days=365)) == {}
    assert record == []


def test_a_cancelled_row_records_when_it_was_settled_and_no_success(
    order: OrderPlaced, record: list[str]
) -> None:
    """``completed_at`` is when it was settled, as for any terminal row, and
    ``succeeded_at`` stays empty: the receiver never ran, so a receiver whose
    work is all cancelled is one that has not been seen to work."""
    event_id = _fired(order)
    before = django_timezone.now()

    cancel_deliveries([event_id])

    row = DeliveryRecord.objects.get(event=event_id, receiver_key=DURABLE)
    assert row.completed_at is not None and row.completed_at >= before
    assert row.succeeded_at is None


def test_a_failed_row_awaiting_its_retry_is_cancelled_too(
    order: OrderPlaced, record: list[str]
) -> None:
    """A scheduled row that came due and failed waits out its backoff as
    FAILED, and "never runs" has to cover its retries. Held separately from
    PENDING because membership in the status list is one branch arc."""
    event_id = _fired(order)
    DeliveryRecord.objects.filter(receiver_key=DURABLE).update(
        status=DeliveryStatus.FAILED,
        attempts=1,
        available_at=django_timezone.now() + timedelta(hours=1),
    )
    DeliveryRecord.objects.filter(receiver_key=CONTEXT).update(status=DeliveryStatus.SUCCEEDED)

    assert cancel_deliveries([event_id]) == {"cancelled": 1, "in_flight": 0}
    assert _statuses(event_id) == {
        DURABLE: DeliveryStatus.CANCELLED,
        CONTEXT: DeliveryStatus.SUCCEEDED,
    }


def test_a_claimed_row_is_left_alone_and_counted(order: OrderPlaced, record: list[str]) -> None:
    """A worker holds it, and may be running the receiver right now: settling it
    under the worker would record an outcome the receiver contradicts. It is
    left to run, and the receiver re-checks as it always did; the count tells
    the caller how many it could not stop."""
    event_id = _fired(order)
    DeliveryRecord.objects.filter(receiver_key=DURABLE).update(
        status=DeliveryStatus.CLAIMED, claimed_by="relay-a"
    )

    assert cancel_deliveries([event_id]) == {"cancelled": 1, "in_flight": 1}
    row = DeliveryRecord.objects.get(event=event_id, receiver_key=DURABLE)
    assert (row.status, row.claimed_by) == (DeliveryStatus.CLAIMED, "relay-a")


@pytest.mark.parametrize(
    "settled",
    [
        DeliveryStatus.SUCCEEDED,
        DeliveryStatus.DEAD,
        DeliveryStatus.ORPHANED,
        DeliveryStatus.CANCELLED,
    ],
)
def test_a_settled_row_keeps_its_outcome(
    order: OrderPlaced, record: list[str], settled: DeliveryStatus
) -> None:
    """Only owed work can be withdrawn. A dead letter cancelled would vanish
    from the dead-letter counts without anyone requeueing or reading it, and a
    success would lose the record that it ran."""
    event_id = _fired(order)
    finished = datetime(2026, 10, 1, tzinfo=timezone.utc)
    DeliveryRecord.objects.update(status=settled, completed_at=finished)

    assert cancel_deliveries([event_id]) == {"cancelled": 0, "in_flight": 0}
    assert set(_statuses(event_id).values()) == {settled}
    assert set(DeliveryRecord.objects.values_list("completed_at", flat=True)) == {stored(finished)}


def test_only_the_named_events_are_touched(order: OrderPlaced, record: list[str]) -> None:
    named = _fired(order)
    other = _fired(order)

    assert cancel_deliveries([named]) == {"cancelled": 2, "in_flight": 0}
    assert set(_statuses(other).values()) == {DeliveryStatus.PENDING}


def test_it_can_be_narrowed_to_one_receiver(order: OrderPlaced, record: list[str]) -> None:
    """One event usually drives an immediate reaction and a scheduled one, and
    only the scheduled one became moot."""
    event_id = _fired(order)

    assert cancel_deliveries([event_id], receiver_key=DURABLE) == {
        "cancelled": 1,
        "in_flight": 0,
    }
    assert _statuses(event_id) == {
        DURABLE: DeliveryStatus.CANCELLED,
        CONTEXT: DeliveryStatus.PENDING,
    }


def test_the_claimed_count_is_narrowed_like_the_cancel(
    order: OrderPlaced, record: list[str]
) -> None:
    """A row held by a worker under another receiver is not one this call
    tried to stop, so it is not reported as left running."""
    event_id = _fired(order)
    DeliveryRecord.objects.filter(receiver_key=CONTEXT).update(status=DeliveryStatus.CLAIMED)

    assert cancel_deliveries([event_id], receiver_key=DURABLE) == {
        "cancelled": 1,
        "in_flight": 0,
    }


def test_it_can_be_narrowed_to_named_rows(order: OrderPlaced, record: list[str]) -> None:
    """An operator reading a list of rows picks the ones they understand."""
    event_id = _fired(order)
    named = DeliveryRecord.objects.get(event=event_id, receiver_key=DURABLE).pk

    assert cancel_deliveries([event_id], delivery_ids=[named]) == {
        "cancelled": 1,
        "in_flight": 0,
    }
    assert _statuses(event_id) == {
        DURABLE: DeliveryStatus.CANCELLED,
        CONTEXT: DeliveryStatus.PENDING,
    }


def test_a_named_row_of_an_event_not_named_is_left_alone(
    order: OrderPlaced, record: list[str]
) -> None:
    """The scopes intersect: naming a row does not widen the call past the
    events it was given, so a stale id list cannot reach another event's work."""
    named = _fired(order)
    other = _fired(order)
    stray = DeliveryRecord.objects.get(event=other, receiver_key=DURABLE).pk

    assert cancel_deliveries([named], delivery_ids=[stray]) == {"cancelled": 0, "in_flight": 0}
    assert set(_statuses(other).values()) == {DeliveryStatus.PENDING}
    assert set(_statuses(named).values()) == {DeliveryStatus.PENDING}


def test_the_receiver_and_row_scopes_intersect(order: OrderPlaced, record: list[str]) -> None:
    event_id = _fired(order)
    ids = list(DeliveryRecord.objects.filter(event=event_id).values_list("pk", flat=True))

    assert cancel_deliveries([event_id], receiver_key=CONTEXT, delivery_ids=ids) == {
        "cancelled": 1,
        "in_flight": 0,
    }
    assert _status(event_id, DURABLE) == DeliveryStatus.PENDING


def test_an_empty_row_list_cancels_nothing(order: OrderPlaced, record: list[str]) -> None:
    """An empty list is a selection of nothing, not the absence of one."""
    event_id = _fired(order)

    assert cancel_deliveries([event_id], delivery_ids=[]) == {"cancelled": 0, "in_flight": 0}
    assert set(_statuses(event_id).values()) == {DeliveryStatus.PENDING}


def test_no_events_and_unknown_events_cancel_nothing(order: OrderPlaced, record: list[str]) -> None:
    event_id = _fired(order)

    assert cancel_deliveries([]) == {"cancelled": 0, "in_flight": 0}
    assert cancel_deliveries([event_id + 1000]) == {"cancelled": 0, "in_flight": 0}
    assert set(_statuses(event_id).values()) == {DeliveryStatus.PENDING}


def test_it_joins_the_caller_s_transaction(order: OrderPlaced, record: list[str]) -> None:
    """Called where the change that made the work moot is written, the cancel
    commits with that change or not at all: an order whose cancellation rolled
    back keeps its reminder."""
    event_id = _fired(order)

    with pytest.raises(RuntimeError), transaction.atomic():
        assert cancel_deliveries([event_id])["cancelled"] == 2
        raise RuntimeError("the change that made it moot failed")

    assert set(_statuses(event_id).values()) == {DeliveryStatus.PENDING}


def test_it_chunks_the_events(order: OrderPlaced, record: list[str], settings: Any) -> None:
    """One transaction per ``BATCH_SIZE`` events: SQLite refuses an unbounded
    parameter list, and a long one would hold every event lock at once."""
    settings.DJANGO_DOMAIN_EVENTS = {"BATCH_SIZE": 2}
    events = [_fired(order) for _ in range(3)]

    with CaptureQueriesContext(connection) as queries:
        assert cancel_deliveries(events) == {"cancelled": 6, "in_flight": 0}

    updates = [q["sql"] for q in queries.captured_queries if q["sql"].startswith("UPDATE")]
    assert len(updates) == 2


def test_it_chunks_the_named_rows(order: OrderPlaced, record: list[str], settings: Any) -> None:
    """The named rows are read and written ``BATCH_SIZE`` at a time, so an
    admin select-all past SQLite's parameter ceiling is several statements
    rather than an OperationalError."""
    settings.DJANGO_DOMAIN_EVENTS = {"BATCH_SIZE": 2}
    event_id = _fired(order)
    ids = list(DeliveryRecord.objects.values_list("pk", flat=True)) + [10_000, 10_001]

    with CaptureQueriesContext(connection) as queries:
        assert cancel_deliveries([event_id], delivery_ids=ids) == {
            "cancelled": 2,
            "in_flight": 0,
        }

    updates = [q["sql"] for q in queries.captured_queries if q["sql"].startswith("UPDATE")]
    assert len(updates) == 1


def test_it_will_not_cancel_a_row_claimed_after_the_lock(
    order: OrderPlaced, record: list[str]
) -> None:
    """The status predicate on the write. A relay's claim never takes the event
    lock, so between this call locking the event and writing, a row can still
    be claimed - hooked here onto the clock read that sits between the two.
    Settling it would record an outcome under a worker about to run it."""
    module = importlib.import_module("django_domain_events.operations.cancel_deliveries")
    event_id = _fired(order)
    stolen_id = DeliveryRecord.objects.get(event=event_id, receiver_key=DURABLE).pk
    real_timezone = module.timezone

    class StealOnClockRead:
        @staticmethod
        def now() -> datetime:
            DeliveryRecord.objects.filter(pk=stolen_id).update(
                status=DeliveryStatus.CLAIMED, claimed_by="relay-b"
            )
            return real_timezone.now()

    with mock.patch.object(module, "timezone", StealOnClockRead):
        assert cancel_deliveries([event_id]) == {"cancelled": 1, "in_flight": 1}

    stolen = DeliveryRecord.objects.get(pk=stolen_id)
    assert (stolen.status, stolen.claimed_by) == (DeliveryStatus.CLAIMED, "relay-b")


def test_a_cancelled_scheduled_event_is_pruned_once_consumed(
    order: OrderPlaced, record: list[str]
) -> None:
    """End to end: cancelling the last owed row is what lets an event under a
    retention policy go at the next sweep, rather than waiting for the work."""
    event_id = _fired(order)
    EventRecord.objects.filter(pk=event_id).update(delete_when="settled")
    assert prune_events() == 0

    cancel_deliveries([event_id])

    assert prune_events() == 1


postgres_only = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="row locks are what is under test; SQLite serialises every writer already",
)


def _in_thread(target: Any) -> tuple[threading.Thread, threading.Event, threading.Event]:
    """Run ``target`` in a transaction on its own connection, holding it open.

    ``target`` runs, then ``inside`` is set and the transaction stays open
    until ``release`` is set, so the main thread acts while this one holds
    every lock it took.
    """
    inside, release = threading.Event(), threading.Event()

    def run() -> None:
        try:
            with transaction.atomic():
                target()
                inside.set()
                release.wait(5)
        finally:
            connection.close()

    thread = threading.Thread(target=run)
    thread.start()
    assert inside.wait(5)
    return thread, inside, release


@postgres_only
def test_a_cancel_waits_for_a_claim_in_flight_and_leaves_the_row_claimed(
    order: OrderPlaced, record: list[str]
) -> None:
    """Two connections. A relay has claimed the row and not committed, so the
    cancel still reads it as pending; its write queues behind the claim's row
    lock and, once the claim commits, re-checks the status and leaves the row
    to the relay. Without the status predicate on the write, the queued write
    lands on the claimed row and settles it under a worker about to run it."""
    event_id = _fired(order)
    claimed: list[int] = []

    def claim() -> None:
        claimed.extend(
            claim_batch(
                worker_id="relay-a",
                now=django_timezone.now(),
                lease=timedelta(minutes=5),
                limit=1,
            )
        )

    thread, _, release = _in_thread(claim)
    timer = threading.Timer(0.5, release.set)
    timer.start()
    try:
        started = time.monotonic()
        counts = cancel_deliveries([event_id])
        waited = time.monotonic() - started
    finally:
        release.set()
        thread.join(10)
        timer.cancel()

    # It queued behind the claim rather than reading the row after it: the
    # interleaving under test happened, not merely the outcome.
    assert waited >= 0.4
    assert counts == {"cancelled": 1, "in_flight": 1}
    row = DeliveryRecord.objects.get(pk=claimed[0])
    assert (row.status, row.claimed_by) == (DeliveryStatus.CLAIMED, "relay-a")


@postgres_only
def test_a_relay_skips_a_row_a_cancel_holds_and_never_claims_it_after(
    order: OrderPlaced, record: list[str]
) -> None:
    """The other order: the cancel has written and not committed. The relay's
    claim skips the locked row rather than waiting, and once the cancel commits
    the row is settled, so no later claim takes it either."""
    event_id = _fired(order)

    thread, _, release = _in_thread(lambda: cancel_deliveries([event_id]))
    try:
        now = django_timezone.now()
        during = claim_batch(worker_id="relay-a", now=now, lease=timedelta(minutes=5), limit=10)
    finally:
        release.set()
        thread.join(10)
    after = claim_batch(
        worker_id="relay-a",
        now=django_timezone.now(),
        lease=timedelta(minutes=5),
        limit=10,
    )

    assert (during, after) == ([], [])
    assert set(_statuses(event_id).values()) == {DeliveryStatus.CANCELLED}


@postgres_only
def test_a_cancel_issued_during_a_replay_is_not_overtaken_by_it(
    order: OrderPlaced, record: list[str]
) -> None:
    """The event lock, observed. A replay holds the event row for its
    transaction, so a cancel issued while it is open waits for it and then
    cancels what it reopened. Without the lock the cancel reads the rows as
    settled, cancels nothing, and the replay commits them owed: the operator is
    told nothing was there to cancel and the work they withdrew runs."""
    from django_domain_events.operations import replay_events as replay_module

    event_id = _fired(order)
    drain_outbox()
    record.clear()
    inside = threading.Event()
    real = replay_module._targets_now
    outcome: dict[str, Any] = {}

    def slow(*args: Any, **kwargs: Any) -> Any:
        inside.set()
        # Long enough for the cancel below to reach the event while the
        # replay's transaction is open.
        threading.Event().wait(0.7)
        return real(*args, **kwargs)

    def replay() -> None:
        try:
            with mock.patch.object(replay_module, "_targets_now", slow):
                outcome["replay"] = replay_module.replay_events([event_id])
        finally:
            connection.close()

    thread = threading.Thread(target=replay)
    thread.start()
    assert inside.wait(5)
    cancelled = cancel_deliveries([event_id])
    thread.join(10)

    assert outcome["replay"] == {"reopened": 2, "added": 0}
    assert cancelled == {"cancelled": 2, "in_flight": 0}
    assert set(_statuses(event_id).values()) == {DeliveryStatus.CANCELLED}


@postgres_only
@pytest.mark.parametrize("batch_size", [None, 1], ids=["whole-events", "chunked"])
def test_a_prune_during_a_cancel_loses_nothing_and_takes_the_event_after(
    order: OrderPlaced, record: list[str], batch_size: int | None
) -> None:
    """Two connections, the cancel open while a prune sweeps. The prune reads
    the event as still owed, because the cancel has not committed, and leaves
    it whole: nothing is lost, and the next sweep takes it.

    This passes with the cancel's event lock removed too, and says so rather
    than claiming to hold it: a prune deletes only an event with nothing owed,
    and a cancel writes only owed rows, so the two never contend for one
    delivery row. The lock is held by the replay test above and by the
    statement-order test below."""
    event_id = _fired(order)
    EventRecord.objects.filter(pk=event_id).update(delete_when="settled")

    thread, _, release = _in_thread(lambda: cancel_deliveries([event_id]))
    try:
        during = prune_events(batch_size=batch_size)
    finally:
        release.set()
        thread.join(10)

    assert during == 0
    assert set(_statuses(event_id).values()) == {DeliveryStatus.CANCELLED}
    assert prune_events(batch_size=batch_size) == 1


@pytest.mark.parametrize("named", [False, True], ids=["by-event", "by-row"])
def test_the_events_are_locked_before_their_rows_are_written(
    order: OrderPlaced, record: list[str], named: bool
) -> None:
    """The lock a replay and a prune also take, taken before the write rather
    than after: the statement order is what makes the wait mean anything. On
    SQLite the lock is no statement at all, so there the order is moot."""
    event_id = _fired(order)
    ids = list(DeliveryRecord.objects.values_list("pk", flat=True)) if named else None

    with CaptureQueriesContext(connection) as queries:
        assert cancel_deliveries([event_id], delivery_ids=ids)["cancelled"] == 2

    statements = [q["sql"] for q in queries.captured_queries]
    updated = next(i for i, sql in enumerate(statements) if sql.startswith("UPDATE"))
    if connection.features.has_select_for_update:
        locked = next(i for i, sql in enumerate(statements) if "FOR UPDATE" in sql)
        assert locked < updated
        # Ascending, so two cancels (and a replay or a prune) locking the same
        # events take them in the same order and cannot deadlock; the backend
        # spells it ``ORDER BY 1 ASC`` or ``ORDER BY "..."."id" ASC``, and a
        # descending lock is the spelling this must not accept.
        assert re.search(r"ORDER BY .*\bASC\b", statements[locked]), statements[locked]
        assert "DESC" not in statements[locked]
