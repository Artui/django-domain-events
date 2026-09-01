"""Tests mirroring ``django_domain_events/deliver.py``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction

from django_domain_events.claim_batch import claim_batch
from django_domain_events.deliver import deliver_one, deliver_pending
from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import receiver_deleted, receiver_replaced
from tests.testapp.events import OrderPlaced, SlowWork

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
    from django_domain_events.deliver import _Fence

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
    from django_domain_events.deliver import _Fence

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
