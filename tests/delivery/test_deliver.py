"""Tests mirroring ``django_domain_events/deliver.py``."""

from __future__ import annotations

import dataclasses
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from django.db import OperationalError, connection, transaction

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_one, deliver_pending
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.utils import partition_by_lane
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.settings import setting
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_deleted, receiver_registered, receiver_replaced
from tests.testapp.events import OrderPlaced, SlowWork, calls

pytestmark = pytest.mark.django_db(transaction=True)


def _fire(order: OrderPlaced) -> None:
    with transaction.atomic():
        fire(order)


def _delivery(key: str) -> DeliveryRecord:
    return DeliveryRecord.objects.select_related("event").get(receiver_key=key)


def _delivery_id(key: str) -> int:
    return DeliveryRecord.objects.values_list("pk", flat=True).get(receiver_key=key)


def test_a_successful_delivery_records_its_outcome(order: OrderPlaced, record: list[str]) -> None:
    _fire(order)
    record.clear()

    assert deliver_one(_delivery_id("testapp.durable_receiver")) is DeliveryStatus.SUCCEEDED

    row = _delivery("testapp.durable_receiver")
    assert (row.status, row.attempts) == (DeliveryStatus.SUCCEEDED, 1)
    assert row.completed_at is not None
    assert record == ["durable:7"]


def test_the_payload_round_trips_through_the_real_codec(
    order: OrderPlaced, record: list[str]
) -> None:
    """Delivery rebuilds the event from the row rather than reusing the instance
    that was fired. A test that passed the original object through would prove
    nothing about what a worker in another process receives."""
    _fire(order)
    record.clear()
    deliver_pending()

    stored = EventRecord.objects.get()
    from django_domain_events.settings import get_codec

    rebuilt = get_codec().decode(OrderPlaced, stored.payload, stored.version)
    assert rebuilt == order


def test_a_receiver_taking_context_is_told_its_attempt(
    order: OrderPlaced, record: list[str]
) -> None:
    _fire(order)
    record.clear()
    deliver_one(_delivery_id("testapp.with_context"))
    assert record == ["context:testapp.OrderPlaced:1"]


def test_a_deleted_receiver_leaves_the_row_orphaned(order: OrderPlaced, record: list[str]) -> None:
    """The cost of freezing the receiver set at fire time. Terminal rather than
    retried: no amount of waiting brings a deleted receiver back."""
    _fire(order)
    with receiver_deleted("testapp.durable_receiver"):
        assert deliver_one(_delivery_id("testapp.durable_receiver")) is DeliveryStatus.ORPHANED

    refreshed = _delivery("testapp.durable_receiver")
    assert refreshed.status == DeliveryStatus.ORPHANED
    assert "renamed, moved or deleted" in refreshed.last_error


def test_a_failing_receiver_is_retried_then_dead_lettered(
    order: OrderPlaced, record: list[str]
) -> None:
    """One failing receiver must not block the other four, which is why the
    delivery row exists per receiver rather than per event."""
    _fire(order)
    row = _delivery("testapp.durable_receiver")
    row.max_attempts = 2
    row.save(update_fields=["max_attempts"])

    def explode(evt: OrderPlaced) -> None:
        raise RuntimeError("downstream is down")

    with receiver_replaced("testapp.durable_receiver", explode):
        assert deliver_one(_delivery_id("testapp.durable_receiver")) is DeliveryStatus.FAILED
        first = _delivery("testapp.durable_receiver")
        assert (first.attempts, first.completed_at) == (1, None)
        assert "downstream is down" in first.last_error

        # Due now: a FAILED row waiting out its backoff is not owed, and
        # deliver_one refuses it for every caller.
        DeliveryRecord.objects.filter(pk=first.pk).update(
            available_at=datetime.now(timezone.utc) - timedelta(seconds=1)
        )
        assert deliver_one(_delivery_id("testapp.durable_receiver")) is DeliveryStatus.DEAD
        second = _delivery("testapp.durable_receiver")
        assert second.attempts == 2
        assert second.completed_at is not None

    # The other durable receiver is untouched by its neighbour's failure.
    assert _delivery("testapp.with_context").status == DeliveryStatus.PENDING


def test_a_receiver_raising_rolls_back_its_own_writes(
    order: OrderPlaced, record: list[str]
) -> None:
    """The property worth advertising: the receiver's work and its acknowledgement
    commit together, so a receiver touching only this database is effectively
    once rather than at-least-once."""
    _fire(order)

    def write_then_explode(evt: OrderPlaced) -> None:
        EventRecord.objects.create(
            name="testapp.side_effect", version=1, payload={}, occurred_at=evt.placed_at
        )
        raise RuntimeError("after the write")

    with receiver_replaced("testapp.durable_receiver", write_then_explode):
        deliver_one(_delivery_id("testapp.durable_receiver"))

    assert not EventRecord.objects.filter(name="testapp.side_effect").exists()


def test_an_undecodable_payload_is_terminal_not_a_stuck_loop(
    order: OrderPlaced, record: list[str]
) -> None:
    """One undecodable row must not stop the other four thousand, and it will not
    decode on the next attempt either."""
    _fire(order)
    stored = EventRecord.objects.get()
    stored.payload = {**stored.payload, "currency": "GBP"}
    stored.save(update_fields=["payload"])

    assert deliver_one(_delivery_id("testapp.durable_receiver")) is DeliveryStatus.DEAD
    row = _delivery("testapp.durable_receiver")
    assert row.status == DeliveryStatus.DEAD
    assert "GBP" in row.last_error


def test_an_unregistered_event_name_fails_the_delivery(
    order: OrderPlaced, record: list[str]
) -> None:
    _fire(order)
    stored = EventRecord.objects.get()
    stored.name = "testapp.NoSuchEvent"
    stored.save(update_fields=["name"])

    assert deliver_one(_delivery_id("testapp.durable_receiver")) is DeliveryStatus.FAILED
    assert "No event is registered" in _delivery("testapp.durable_receiver").last_error


def test_deliver_pending_reports_what_it_did(order: OrderPlaced, record: list[str]) -> None:
    _fire(order)
    record.clear()
    assert deliver_pending() == {DeliveryStatus.SUCCEEDED: 2}
    assert deliver_pending() == {}


def test_deliver_pending_honours_a_limit(order: OrderPlaced, record: list[str]) -> None:
    _fire(order)
    record.clear()
    assert deliver_pending(limit=1) == {DeliveryStatus.SUCCEEDED: 1}
    assert DeliveryRecord.objects.filter(status=DeliveryStatus.PENDING).count() == 1


def test_a_failed_delivery_is_picked_up_by_the_next_pass(
    order: OrderPlaced, record: list[str]
) -> None:
    """FAILED is distinct from PENDING so that "has this ever failed" is
    answerable, but both are owed and both get claimed."""
    _fire(order)
    row = _delivery("testapp.durable_receiver")
    row.status = DeliveryStatus.FAILED
    row.attempts = 1
    row.save(update_fields=["status", "attempts"])
    record.clear()

    assert deliver_pending() == {DeliveryStatus.SUCCEEDED: 2}


def test_a_failed_delivery_is_retried_through_a_real_claim(
    order: OrderPlaced, record: list[str]
) -> None:
    """Fail a delivery through _fail, then re-claim it the way a worker does.

    The gap this closes is why a regression shipped: backoff writes available_at
    and the claim filters on it, but every existing test drove one or the other.
    One called deliver_one directly, bypassing the claim; another hand-wrote
    FAILED without moving available_at. Composed, they did not work at all.
    """
    calls: list[int] = []

    def flaky(evt: OrderPlaced) -> None:
        calls.append(1)
        if len(calls) < 2:
            raise RuntimeError("transient")

    with receiver_replaced("testapp.durable_receiver", flaky):
        with transaction.atomic():
            fire(order)
        for _ in range(5):
            drain_outbox()

    assert len(calls) == 2, "the failed delivery was never retried"
    assert _delivery("testapp.durable_receiver").status == DeliveryStatus.SUCCEEDED


def test_drain_delivers_everything_owed_not_one_batch(
    order: OrderPlaced, record: list[str]
) -> None:
    """The helper says "to completion" and a batch size is an implementation
    detail of the claim, not a cap on what a caller asked for."""
    from django_domain_events.settings import setting

    fan_out = 2
    events = setting("BATCH_SIZE") + 5
    with transaction.atomic():
        for _ in range(events):
            fire(order)
    record.clear()

    drain_outbox()

    assert DeliveryRecord.objects.filter(status=DeliveryStatus.PENDING).count() == 0
    assert DeliveryRecord.objects.count() == events * fan_out


def test_a_write_conditioned_on_a_lapsed_claim_lands_nowhere(
    order: OrderPlaced, record: list[str]
) -> None:
    """A lease can lapse while its worker is still alive and mid-receiver.

    The fence is captured when the delivery is read, so a worker that has since
    lost the row writes nothing: without it a zombie overwrites the verdict of
    whoever legitimately took it, resurrecting a SUCCEEDED row and resetting the
    attempt budget that makes max_attempts mean anything.
    """
    from django_domain_events.delivery.deliver import _Fence

    with transaction.atomic():
        fire(order)
    claim_batch(
        worker_id="A", now=datetime.now(timezone.utc), lease=timedelta(seconds=-1), limit=10
    )
    zombie = _Fence(_delivery("testapp.durable_receiver"))

    claim_batch(worker_id="B", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)

    assert zombie.write(status=DeliveryStatus.FAILED, attempts=99) is None
    assert zombie.extend_lease(datetime.now(timezone.utc)) is False

    row = _delivery("testapp.durable_receiver")
    assert (row.claimed_by, row.attempts) == ("B", 0)


def test_the_receiver_work_rolls_back_when_the_claim_is_lost(
    order: OrderPlaced, record: list[str]
) -> None:
    """Losing the row mid-flight must not leave the receiver's writes committed
    with no acknowledgement to match: whoever holds the claim will deliver it
    again, and the effects would then be doubled."""
    with transaction.atomic():
        fire(order)
    delivery_id = _delivery_id("testapp.durable_receiver")
    claim_batch(worker_id="A", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)

    def steal_then_write(evt: OrderPlaced) -> None:
        DeliveryRecord.objects.filter(pk=delivery_id).update(
            claimed_by="B", claimed_at=datetime.now(timezone.utc)
        )
        EventRecord.objects.create(
            name="testapp.side_effect", version=1, payload={}, occurred_at=evt.placed_at
        )

    with receiver_replaced("testapp.durable_receiver", steal_then_write):
        assert deliver_one(delivery_id, worker_id="A") is None

    assert not EventRecord.objects.filter(name="testapp.side_effect").exists()
    assert _delivery("testapp.durable_receiver").status == DeliveryStatus.CLAIMED


def test_the_lease_is_extended_to_cover_the_delivery_it_is_about_to_run(
    order: OrderPlaced, record: list[str]
) -> None:
    """A batch claim stamps one expiry across every row it took and the relay
    delivers them serially, so without this the lease is a budget for the whole
    batch and runs out partway through."""
    with transaction.atomic():
        fire(order)
    claim_batch(
        worker_id="w1", now=datetime.now(timezone.utc), lease=timedelta(seconds=1), limit=10
    )
    before = _delivery("testapp.durable_receiver").lease_expires_at

    deliver_one(_delivery_id("testapp.durable_receiver"), worker_id="w1")

    assert _delivery("testapp.durable_receiver").lease_expires_at > before


def test_a_worker_whose_lease_already_lapsed_delivers_nothing(
    order: OrderPlaced, record: list[str]
) -> None:
    """The check happens before the receiver runs, not after: a worker that has
    already lost the row should not do the work at all."""
    with transaction.atomic():
        fire(order)
    delivery_id = _delivery_id("testapp.durable_receiver")
    claim_batch(worker_id="A", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)
    DeliveryRecord.objects.filter(pk=delivery_id).update(
        claimed_by="B", claimed_at=datetime.now(timezone.utc)
    )
    record.clear()

    assert deliver_one(delivery_id, worker_id="A") is None
    assert record == []


def test_deliver_pending_skips_rows_it_lost(order: OrderPlaced, record: list[str]) -> None:
    """A lost row is not an outcome to count. It is still owed, and whoever
    holds the claim will report it.

    Two events, so a lost row falls in the middle of the batch rather than at
    its end: a loop that only ever loses its last row never exercises carrying
    on. An earlier version of this test also marked rows CLAIMED with no lease,
    so nothing was claimable and the loop it meant to exercise never ran at all.
    """

    def steal_everything_else(evt: OrderPlaced) -> None:
        DeliveryRecord.objects.exclude(receiver_key="testapp.durable_receiver").update(
            claimed_by="someone-else", claimed_at=datetime.now(timezone.utc)
        )

    with transaction.atomic():
        fire(order)
        fire(order)
    with receiver_replaced("testapp.durable_receiver", steal_everything_else):
        counts = deliver_pending()

    # Derived rather than hardcoded: the fan-out depends on how many receivers
    # are registered, which other tests legitimately change.
    # Deterministic regardless of how many receivers are registered: the first
    # row delivered succeeds and takes every other row away from this worker, so
    # the rest are lost mid-pass rather than at its end.
    lost = DeliveryRecord.objects.filter(claimed_by="someone-else").count()
    assert lost >= 2, "nothing was lost mid-pass, so the loop was never resumed"
    assert counts == {DeliveryStatus.SUCCEEDED: 2}


def test_a_write_conditioned_on_a_lapsed_claim_lands_nowhere(
    order: OrderPlaced, record: list[str]
) -> None:
    """A lease can lapse while its worker is still alive and mid-receiver.

    The fence is captured when the delivery is read, so a worker that has since
    lost the row writes nothing: without it a zombie overwrites the verdict of
    whoever legitimately took it, resurrecting a SUCCEEDED row and resetting the
    attempt budget that makes max_attempts mean anything.
    """
    from django_domain_events.delivery.deliver import _Fence

    with transaction.atomic():
        fire(order)
    claim_batch(
        worker_id="A", now=datetime.now(timezone.utc), lease=timedelta(seconds=-1), limit=10
    )
    zombie = _Fence(_delivery("testapp.durable_receiver"))

    claim_batch(worker_id="B", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)

    assert zombie.write(status=DeliveryStatus.FAILED, attempts=99) is None
    assert zombie.extend_lease(datetime.now(timezone.utc)) is False

    row = _delivery("testapp.durable_receiver")
    assert (row.claimed_by, row.attempts) == ("B", 0)


def test_the_receiver_work_rolls_back_when_the_claim_is_lost(
    order: OrderPlaced, record: list[str]
) -> None:
    """Losing the row mid-flight must not leave the receiver's writes committed
    with no acknowledgement to match: whoever holds the claim will deliver it
    again, and the effects would then be doubled."""
    with transaction.atomic():
        fire(order)
    delivery_id = _delivery_id("testapp.durable_receiver")
    claim_batch(worker_id="A", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)

    def steal_then_write(evt: OrderPlaced) -> None:
        DeliveryRecord.objects.filter(pk=delivery_id).update(
            claimed_by="B", claimed_at=datetime.now(timezone.utc)
        )
        EventRecord.objects.create(
            name="testapp.side_effect", version=1, payload={}, occurred_at=evt.placed_at
        )

    with receiver_replaced("testapp.durable_receiver", steal_then_write):
        assert deliver_one(delivery_id, worker_id="A") is None

    assert not EventRecord.objects.filter(name="testapp.side_effect").exists()
    assert _delivery("testapp.durable_receiver").status == DeliveryStatus.CLAIMED


def test_the_lease_is_extended_to_cover_the_delivery_it_is_about_to_run(
    order: OrderPlaced, record: list[str]
) -> None:
    """A batch claim stamps one expiry across every row it took and the relay
    delivers them serially, so without this the lease is a budget for the whole
    batch and runs out partway through."""
    with transaction.atomic():
        fire(order)
    claim_batch(
        worker_id="w1", now=datetime.now(timezone.utc), lease=timedelta(seconds=1), limit=10
    )
    before = _delivery("testapp.durable_receiver").lease_expires_at

    deliver_one(_delivery_id("testapp.durable_receiver"), worker_id="w1")

    assert _delivery("testapp.durable_receiver").lease_expires_at > before


def test_a_worker_whose_lease_already_lapsed_delivers_nothing(
    order: OrderPlaced, record: list[str]
) -> None:
    """The check happens before the receiver runs, not after: a worker that has
    already lost the row should not do the work at all."""
    with transaction.atomic():
        fire(order)
    delivery_id = _delivery_id("testapp.durable_receiver")
    claim_batch(worker_id="A", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)
    DeliveryRecord.objects.filter(pk=delivery_id).update(
        claimed_by="B", claimed_at=datetime.now(timezone.utc)
    )
    record.clear()

    assert deliver_one(delivery_id, worker_id="A") is None
    assert record == []


@pytest.mark.django_db
def test_a_receiver_can_declare_a_longer_lease(order: OrderPlaced, record: list[str]) -> None:
    """The answer for a receiver that legitimately runs long, and the only one
    available: it cannot extend its own lease, because it runs inside the
    transaction that carries its acknowledgement and nothing it writes is
    visible to another worker until it has already finished."""
    with transaction.atomic():
        fire(SlowWork(value=1))
    row = DeliveryRecord.objects.filter(receiver_key="testapp.slow_receiver").get()
    now = datetime.now(timezone.utc)
    claim_batch(worker_id="w1", now=now, lease=timedelta(seconds=1), limit=10)

    deliver_one(row.pk, worker_id="w1")

    ran = DeliveryRecord.objects.get(pk=row.pk)
    assert ran.status == DeliveryStatus.SUCCEEDED
    assert ran.lease_expires_at is not None
    # Well past the setting, not merely past it: deliver_one reads its own
    # clock a moment after this one, so `> now + LEASE_SECONDS` is true by
    # microseconds even when the override is ignored entirely.
    assert ran.lease_expires_at > now + timedelta(seconds=setting("LEASE_SECONDS") * 3)


@pytest.mark.django_db
def test_a_receiver_without_one_gets_the_setting(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    row = DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").get()
    now = datetime.now(timezone.utc)
    claim_batch(worker_id="w1", now=now, lease=timedelta(seconds=1), limit=10)

    captured = []
    original = DeliveryRecord.objects.get(pk=row.pk)

    def watch(evt):
        captured.append(DeliveryRecord.objects.get(pk=row.pk).lease_expires_at)

    with receiver_replaced("testapp.durable_receiver", watch):
        deliver_one(row.pk, worker_id="w1")

    assert original.lease_expires_at is not None
    assert captured[0] is not None
    assert captured[0] <= now + timedelta(seconds=setting("LEASE_SECONDS") + 5)


@pytest.mark.django_db
def test_an_orphaned_row_still_extends_before_it_is_written(
    order: OrderPlaced, record: list[str]
) -> None:
    """The receiver is resolved before the lease so it can size it, and a
    missing receiver must not skip re-establishing ownership: the ORPHANED
    write is still a write."""
    with transaction.atomic():
        fire(order)
    row = DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").get()
    claim_batch(
        worker_id="w1", now=datetime.now(timezone.utc), lease=timedelta(seconds=60), limit=10
    )

    with receiver_deleted("testapp.durable_receiver"):
        assert deliver_one(row.pk, worker_id="w1") == DeliveryStatus.ORPHANED

    # And a worker that no longer holds the row writes nothing, orphan or not.
    DeliveryRecord.objects.filter(pk=row.pk).update(
        status=DeliveryStatus.CLAIMED, claimed_by="someone-else", last_error=""
    )
    with receiver_deleted("testapp.durable_receiver"):
        assert deliver_one(row.pk, worker_id="w1") is None
    assert DeliveryRecord.objects.get(pk=row.pk).status == DeliveryStatus.CLAIMED


@pytest.mark.django_db(transaction=True)
def test_a_receivers_own_write_is_invisible_to_another_worker_until_it_commits(
    order: OrderPlaced, record: list[str]
) -> None:
    """Why lease_seconds= exists instead of an extend_lease() a receiver could
    call. The receiver runs inside the transaction carrying its
    acknowledgement, so a lease it pushes out is not published until it has
    already finished - by which point the fence has decided the outcome.

    Two real connections, because one connection cannot observe its own
    isolation. Meaningful only where readers are not blocked outright, so it
    asserts staleness on Postgres and mere non-visibility everywhere.

    This records a measurement rather than gating a line of ours: no edit to
    this package makes it fail, because what it observes is the database's
    isolation. It is here so the claim that justifies lease_seconds= is
    reproducible instead of asserted in a docstring.
    """
    with transaction.atomic():
        fire(order)
    row = DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").get()
    started = datetime.now(timezone.utc)
    claim_batch(worker_id="w1", now=started, lease=timedelta(seconds=1), limit=10)

    seen: dict[str, object] = {}

    def receiver_that_extends_its_own_lease(evt: OrderPlaced) -> None:
        DeliveryRecord.objects.filter(pk=row.pk).update(
            lease_expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
        )

        def other_worker() -> None:
            connection.close()
            try:
                seen["lease"] = DeliveryRecord.objects.filter(pk=row.pk).values_list(
                    "lease_expires_at", flat=True
                )[0]
            except OperationalError as exc:
                # SQLite locks the table rather than serving a stale read. Both
                # answers make the same point: the extension is not published.
                seen["lease"] = exc
            finally:
                connection.close()

        thread = threading.Thread(target=other_worker)
        thread.start()
        thread.join()

    with receiver_replaced("testapp.durable_receiver", receiver_that_extends_its_own_lease):
        deliver_one(row.pk, worker_id="w1")

    observed = seen["lease"]
    if isinstance(observed, OperationalError):
        return
    # Not compared against the claim lease: deliver_one publishes its own
    # extension before the receiver starts, and that one the other worker does
    # see. What it must not see is the hour the receiver added on top.
    assert isinstance(observed, datetime)
    assert observed < started + timedelta(minutes=30), (
        "another worker must not see the extension the receiver wrote"
    )
    assert DeliveryRecord.objects.get(pk=row.pk).lease_expires_at > started + timedelta(
        minutes=30
    ), "and yet it did land, once the receiver had already finished"


def _set(key: str, **fields: object) -> int:
    """Hand-write a row into a state, the way a stale or repeated message finds it."""
    delivery_id = _delivery_id(key)
    DeliveryRecord.objects.filter(pk=delivery_id).update(**fields)
    return delivery_id


def test_a_settled_row_delivered_again_does_not_run_its_receiver_twice(
    order: OrderPlaced, record: list[str]
) -> None:
    """A queue that redelivers - ``acks_late`` and a broker's visibility timeout
    both do - hands ``deliver_one`` a row that already succeeded. Running it
    again repeats the receiver's work and the acknowledgement it committed with,
    which breaks the one promise that makes a database-only receiver effectively
    once."""
    _fire(order)
    delivery_id = _delivery_id("testapp.durable_receiver")
    assert deliver_one(delivery_id) is DeliveryStatus.SUCCEEDED
    record.clear()

    assert deliver_one(delivery_id) is None

    assert record == []
    assert _delivery("testapp.durable_receiver").attempts == 1


def test_a_dead_row_is_not_run_or_resurrected(order: OrderPlaced, record: list[str]) -> None:
    """A dead letter is final. Running it again spends a sixth attempt of five
    and writes SUCCEEDED over a row whose dead-letter notice has already gone
    out, so the two records of one delivery disagree."""
    _fire(order)
    delivery_id = _set(
        "testapp.durable_receiver",
        status=DeliveryStatus.DEAD,
        attempts=5,
        max_attempts=5,
        completed_at=datetime.now(timezone.utc),
    )
    record.clear()

    assert deliver_one(delivery_id) is None

    assert record == []
    row = _delivery("testapp.durable_receiver")
    assert (row.status, row.attempts) == (DeliveryStatus.DEAD, 5)


def test_an_orphaned_row_is_not_run(order: OrderPlaced, record: list[str]) -> None:
    """The third terminal status, held by its own test: the refusal reads a set,
    and dropping one member from it leaves the other two tests green."""
    _fire(order)
    delivery_id = _set(
        "testapp.durable_receiver",
        status=DeliveryStatus.ORPHANED,
        completed_at=datetime.now(timezone.utc),
    )
    record.clear()

    assert deliver_one(delivery_id) is None

    assert record == []
    assert _delivery("testapp.durable_receiver").status == DeliveryStatus.ORPHANED


def test_a_failed_row_not_yet_due_is_not_run(order: OrderPlaced, record: list[str]) -> None:
    """``available_at`` is the backoff, and a ``RetryAfter`` the receiver raised
    writes it too. Running the row early ignores both - and for a destination
    that answered 429 with an hour, that is the call it asked not to get."""
    _fire(order)
    delivery_id = _set(
        "testapp.durable_receiver",
        status=DeliveryStatus.FAILED,
        attempts=1,
        available_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    record.clear()

    assert deliver_one(delivery_id) is None

    assert record == []
    row = _delivery("testapp.durable_receiver")
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)


def test_a_failed_row_that_is_due_still_runs(order: OrderPlaced, record: list[str]) -> None:
    """The other half of the backoff check: refusing every FAILED row would pass
    the test above and strand every retry."""
    _fire(order)
    delivery_id = _set(
        "testapp.durable_receiver",
        status=DeliveryStatus.FAILED,
        attempts=1,
        available_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    record.clear()

    assert deliver_one(delivery_id) is DeliveryStatus.SUCCEEDED
    assert record == ["durable:7"]


def test_half_a_claim_takes_nothing(order: OrderPlaced, record: list[str]) -> None:
    """Either half of a claim on its own still triggers the take, and the take
    cannot match a row with a missing half. Without that, a backend that dropped
    one argument would fall through to running the row unconditionally - the
    redelivery bug, reintroduced by a typo."""
    with transaction.atomic():
        fire(order)
    claim_batch(worker_id="w1", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)
    row = _delivery("testapp.durable_receiver")
    record.clear()

    assert deliver_one(row.pk, claimed_by="w1") is None
    assert deliver_one(row.pk, claimed_at=row.claimed_at.isoformat()) is None

    assert record == []
    assert _delivery("testapp.durable_receiver").claimed_by == "w1"


def test_a_take_moves_the_row_to_the_named_worker(order: OrderPlaced, record: list[str]) -> None:
    """``worker_id`` names who takes the row, so a task worker can log under a
    name of its own; the claim it was handed is what it takes the row from."""
    with transaction.atomic():
        fire(order)
    claim_batch(worker_id="w1", now=datetime.now(timezone.utc), lease=timedelta(hours=1), limit=10)
    row = _delivery("testapp.durable_receiver")
    record.clear()

    outcome = deliver_one(
        row.pk, worker_id="celery-7", claimed_by="w1", claimed_at=row.claimed_at.isoformat()
    )

    assert outcome is DeliveryStatus.SUCCEEDED
    after = _delivery("testapp.durable_receiver")
    assert after.claimed_by == "celery-7"
    assert after.claimed_at > row.claimed_at


def test_a_failed_row_claimed_early_on_purpose_still_runs(
    order: OrderPlaced, record: list[str]
) -> None:
    """``ignore_backoff=True`` claims a row before its ``available_at`` because
    an operator asked for exactly that. The claim makes it CLAIMED, and a
    CLAIMED row is its claimer's to run whatever its backoff said - which is
    why the backoff check reads the FAILED status and not ``available_at``
    alone."""
    _fire(order)
    _set(
        "testapp.durable_receiver",
        status=DeliveryStatus.FAILED,
        attempts=1,
        available_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    _set("testapp.with_context", status=DeliveryStatus.SUCCEEDED)
    record.clear()

    assert deliver_pending(ignore_backoff=True) == {DeliveryStatus.SUCCEEDED: 1}
    assert record == ["durable:7"]


@contextmanager
def _declared(key: str, **fields: object) -> Iterator[None]:
    """Change what one registered receiver declares, for the duration of a test."""
    entry = registry.receiver_for_key(key)
    original = {name: getattr(entry, name) for name in fields}
    for name, value in fields.items():
        object.__setattr__(entry, name, value)
    try:
        yield
    finally:
        for name, value in original.items():
            object.__setattr__(entry, name, value)


def _explode(evt: OrderPlaced) -> None:
    raise RuntimeError("downstream is down")


def _wait_after_failing(key: str) -> float:
    """Fail one attempt of ``key`` and return the delay it was scheduled for."""
    before = datetime.now(timezone.utc)
    with receiver_replaced(key, _explode):
        assert deliver_one(_delivery_id(key)) is DeliveryStatus.FAILED
    return (_delivery(key).available_at - before).total_seconds()


def test_a_receiver_can_declare_its_own_backoff_base(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read when the attempt fails, from the registry - the row was fired
    before the curve was declared, which is the case of a deploy changing it
    under deliveries already in flight."""
    monkeypatch.setattr(deliver_module.random, "random", lambda: 1.0)
    _fire(order)

    with _declared("testapp.durable_receiver", backoff_base_seconds=600):
        waited = _wait_after_failing("testapp.durable_receiver")

    assert 595 < waited < 605
    assert _wait_after_failing("testapp.with_context") < setting("BACKOFF_BASE_SECONDS") + 5


def test_a_receiver_can_declare_its_own_backoff_cap(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ten attempts in, the setting's base of 2 seconds has doubled to 1024,
    under the setting's cap of an hour and well over this receiver's 30."""
    monkeypatch.setattr(deliver_module.random, "random", lambda: 1.0)
    _fire(order)
    _set("testapp.durable_receiver", attempts=9, max_attempts=20)
    _set("testapp.with_context", attempts=9, max_attempts=20)

    with _declared("testapp.durable_receiver", backoff_cap_seconds=30):
        waited = _wait_after_failing("testapp.durable_receiver")

    assert 25 < waited < 35
    assert _wait_after_failing("testapp.with_context") > 1000


def test_the_delivery_id_reaches_a_receiver_taking_context(
    order: OrderPlaced, record: list[str]
) -> None:
    """The id of the row being delivered, which is what a receiver needs to
    correlate its own record with the outbox's."""
    _fire(order)
    seen: list[DeliveryContext] = []

    with receiver_replaced("testapp.with_context", lambda evt, ctx: seen.append(ctx)):
        deliver_one(_delivery_id("testapp.with_context"))

    assert [ctx.delivery_id for ctx in seen] == [_delivery_id("testapp.with_context")]


def _laned_mail() -> RegisteredReceiver:
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


def test_deliver_pending_serves_the_lane_it_is_given(order: OrderPlaced, record: list[str]) -> None:
    with receiver_registered(_laned_mail()):
        _fire(order)
        record.clear()

        assert deliver_pending(lane="mail") == {DeliveryStatus.SUCCEEDED: 1}
        assert record == ["mail"]
        assert deliver_pending(lane="default") == {DeliveryStatus.SUCCEEDED: 2}
        assert "mail" not in record[1:]


def test_deliver_pending_with_no_lane_serves_every_lane(
    order: OrderPlaced, record: list[str]
) -> None:
    """Its contract before lanes, kept: drain_outbox() is built on it and
    promises everything owed."""
    with receiver_registered(_laned_mail()):
        _fire(order)

        assert deliver_pending() == {DeliveryStatus.SUCCEEDED: 3}


def _throttled_mail(seen: list[int]) -> RegisteredReceiver:
    """A mail-lane receiver whose destination is throttling: every call defers
    without counting. Stops with a BaseException past five calls, so a pass
    that kept claiming its rows fails rather than hangs."""

    def throttled(evt: OrderPlaced) -> None:
        seen.append(evt.order_id)
        if len(seen) > 5:
            raise _Runaway("a throttled lane kept being claimed")
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


class _Runaway(BaseException):
    """A BaseException, so delivery does not record it as a failure."""


def _given_back(key: str) -> list[int]:
    """The rows of ``key`` handed back: still CLAIMED, with a lease in the past."""
    return sorted(
        DeliveryRecord.objects.filter(
            receiver_key=key,
            status=DeliveryStatus.CLAIMED,
            lease_expires_at__lt=datetime.now(timezone.utc),
        ).values_list("pk", flat=True)
    )


def test_a_deferral_sets_its_lane_aside_and_the_pass_delivers_the_rest(
    order: OrderPlaced, record: list[str]
) -> None:
    """Serving every lane, the claimed batch mixes them. The deferring lane's
    unstarted rows are handed back rather than attempted, and the other lanes'
    rows in the same batch are still delivered."""
    seen: list[int] = []
    with receiver_registered(_throttled_mail(seen)):
        for _ in range(3):
            _fire(order)

        assert deliver_pending() == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 6}

        assert len(seen) == 1, "the throttled lane's other rows were attempted"
        handed_back = _given_back("tests.throttled")
        assert len(handed_back) == 2
        taken = claim_batch(
            worker_id="w2",
            now=datetime.now(timezone.utc),
            lease=timedelta(minutes=5),
            limit=10,
            lane="mail",
        )
        assert sorted(taken) == handed_back, "another worker could not take them at once"


def test_a_deferral_in_the_lane_a_pass_serves_ends_the_pass(
    order: OrderPlaced, record: list[str]
) -> None:
    seen: list[int] = []
    with receiver_registered(_throttled_mail(seen)):
        for _ in range(3):
            _fire(order)

        assert deliver_pending(lane="mail") == {DeliveryStatus.FAILED: 1}

        assert len(seen) == 1
        assert len(_given_back("tests.throttled")) == 2


def test_a_deferral_is_reported_to_whoever_dispatched_it(order: OrderPlaced) -> None:
    """With its lane and the delay the destination asked for, not the jittered
    one: a pause longer than asked would hold back rows the destination is
    ready for."""
    paused: list[tuple[str, float]] = []
    with receiver_registered(_throttled_mail([])):
        _fire(order)
        [row_id] = claim_batch(
            worker_id="w1",
            now=datetime.now(timezone.utc),
            lease=timedelta(minutes=5),
            limit=10,
            lane="mail",
        )
        outcome = deliver_module.dispatch_one(
            row_id, worker_id="w1", on_deferral=lambda lane, s: paused.append((lane, s))
        )

    assert outcome == DeliveryStatus.FAILED
    assert paused == [("mail", 30.0)]


def test_a_deferral_this_worker_could_not_record_pauses_nothing(
    order: OrderPlaced, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A worker that lost the row has no say over its lane: whoever holds the
    row now decides what its deferral means."""
    paused: list[tuple[str, float]] = []
    with receiver_registered(_throttled_mail([])):
        _fire(order)
        [row_id] = claim_batch(
            worker_id="w1",
            now=datetime.now(timezone.utc),
            lease=timedelta(minutes=5),
            limit=10,
            lane="mail",
        )
        monkeypatch.setattr(deliver_module._Fence, "write", lambda self, **fields: None)
        outcome = deliver_module.dispatch_one(
            row_id, worker_id="w1", on_deferral=lambda lane, s: paused.append((lane, s))
        )

    assert outcome is None
    assert paused == []


def _claimed(lane: str) -> int:
    [row_id] = claim_batch(
        worker_id="w1",
        now=datetime.now(timezone.utc),
        lease=timedelta(minutes=5),
        limit=10,
        lane=lane,
    )
    return row_id


def test_a_deferral_run_directly_is_recorded_and_pauses_nothing(order: OrderPlaced) -> None:
    """``deliver_one`` holds no batch, so there is nothing to pause or hand
    back; the deferral is still recorded as it is anywhere else."""
    with receiver_registered(_throttled_mail([])):
        _fire(order)
        row_id = _claimed("mail")

        assert deliver_one(row_id, worker_id="w1") == DeliveryStatus.FAILED

    row = DeliveryRecord.objects.get(pk=row_id)
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 0)


def test_a_deferral_inside_a_task_is_recorded(order: OrderPlaced) -> None:
    """A task runs ``deliver_one`` with the claim its message carried, in a
    process holding no batch: nothing to pause, and the deferral recorded all
    the same."""
    with receiver_registered(_throttled_mail([])):
        _fire(order)
        row_id = _claimed("mail")
        row = DeliveryRecord.objects.get(pk=row_id)

        outcome = deliver_one(
            row_id, claimed_by=row.claimed_by, claimed_at=row.claimed_at.isoformat()
        )

    assert outcome == DeliveryStatus.FAILED
    row.refresh_from_db()
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 0)


def test_a_give_up_this_worker_could_not_record_tells_on_failure_nothing(
    order: OrderPlaced, monkeypatch: pytest.MonkeyPatch
) -> None:
    """As with every other outcome: the worker holding the row now is the one
    that will record it, and telling the hook twice would log it twice."""
    heard: list[object] = []
    bounded = dataclasses.replace(_throttled_mail([]), on_failure=heard.append)
    with receiver_registered(bounded):
        _fire(order)
        EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(days=2))
        row_id = _claimed("mail")
        monkeypatch.setattr(deliver_module._Fence, "write", lambda self, **fields: None)

        assert deliver_one(row_id, worker_id="w1") is None

    assert heard == []


def test_a_deleted_receivers_row_is_in_the_default_lane(order: OrderPlaced) -> None:
    """As the claim reads it: a row whose receiver is gone names no lane, and
    it drains through the default one, so a default-lane deferral hands it back
    with the rest."""
    with receiver_registered(_laned_mail()):
        _fire(order)
    ids = list(DeliveryRecord.objects.order_by("pk").values_list("pk", flat=True))
    gone = _delivery_id("tests.mail")

    with receiver_deleted("testapp.durable_receiver"):
        inside, rest = partition_by_lane(ids, "default")

    assert inside == ids, "a row with no receiver was not counted in the default lane"
    assert rest == []
    assert gone in inside


def test_deliver_pending_refuses_a_lane_nobody_declared(order: OrderPlaced) -> None:
    with pytest.raises(ValueError, match="No receiver is declared in lane 'mial'"):
        deliver_pending(lane="mial")


def test_deliver_pending_claims_in_batches_of_the_size_it_is_given(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Overriding BATCH_SIZE for its claims alone, and still draining everything
    owed when no limit is set."""
    limits: list[int] = []
    real = deliver_module.claim_batch

    def spy(**kwargs: Any) -> list[int]:
        limits.append(kwargs["limit"])
        return real(**kwargs)

    monkeypatch.setattr(deliver_module, "claim_batch", spy)
    _fire(order)

    assert deliver_pending(batch_size=1) == {DeliveryStatus.SUCCEEDED: 2}
    assert limits == [1, 1, 1]


def test_a_limit_and_a_batch_size_together_are_refused(
    order: OrderPlaced, record: list[str]
) -> None:
    """A limit is one claim of that many rows, so the batch would be ignored -
    and claiming a hundred rows under one lease is what a small batch is asked
    for to prevent. Each alone is accepted, which holds both conjuncts."""
    _fire(order)

    with pytest.raises(ValueError, match="limit=100 is one claim of that many rows"):
        deliver_pending(limit=100, batch_size=5)
    assert deliver_pending(limit=1) == {DeliveryStatus.SUCCEEDED: 1}
    assert deliver_pending(batch_size=5) == {DeliveryStatus.SUCCEEDED: 1}


@pytest.mark.parametrize("size", [0, -1])
def test_deliver_pending_refuses_a_batch_that_claims_nothing(size: int) -> None:
    """A batch of zero claims nothing on every pass, so the loop it sizes ends
    at once having delivered nothing, and a relay sized that way idles forever."""
    with pytest.raises(ValueError, match="batch_size must be positive"):
        deliver_pending(batch_size=size)
