"""Tests mirroring ``django_domain_events/deliver.py``."""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from django.db import transaction

from django_domain_events.deliver import deliver_one, deliver_pending
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.registry import registry
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

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
    with _receiver_deleted("testapp.durable_receiver"):
        assert deliver_one(_delivery_id("testapp.durable_receiver")) is DeliveryStatus.ORPHANED

    refreshed = _delivery("testapp.durable_receiver")
    assert refreshed.status == DeliveryStatus.ORPHANED
    assert "renamed, moved or deleted" in refreshed.last_error


@contextmanager
def _receiver_deleted(key: str):
    """Take a receiver out of the live registry, and put it back.

    Reaching into the registry's dict rather than reloading the declaring
    module: a reload rebuilds the classes and functions, so re-registering them
    collides with the originals under the same names and corrupts the registry
    for every test that follows.
    """
    removed = registry._receivers.pop(key)
    try:
        yield
    finally:
        registry._receivers[key] = removed


@contextmanager
def _receiver_replaced(key: str, func):
    """Swap one receiver's callable for the duration of a test."""
    entry = registry.receiver_for_key(key)
    original = entry.func
    object.__setattr__(entry, "func", func)
    try:
        yield
    finally:
        object.__setattr__(entry, "func", original)


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

    with _receiver_replaced("testapp.durable_receiver", explode):
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

    with _receiver_replaced("testapp.durable_receiver", write_then_explode):
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
