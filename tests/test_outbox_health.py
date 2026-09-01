"""Tests mirroring ``django_domain_events/outbox_health.py``."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.outbox_health import outbox_health
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db


def test_an_empty_outbox_is_all_zeroes_and_no_timestamp() -> None:
    health = outbox_health()
    assert (health.owed, health.claimed, health.dead, health.lapsed_leases) == (0, 0, 0, 0)
    assert health.oldest_owed_at is None
    assert health.receivers == ()


def test_owed_counts_what_the_relay_will_pick_up(order: OrderPlaced, record: list[str]) -> None:
    """Owed means not terminal, the same definition the relay claims by, so
    this cannot say the queue is empty while the relay still has work."""
    with transaction.atomic():
        fire(order)
    assert outbox_health().owed == 2

    drain_outbox()
    assert outbox_health().owed == 0


def test_a_claimed_row_is_still_owed(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.CLAIMED, claimed_by="w1")
    health = outbox_health()
    assert (health.owed, health.claimed) == (2, 2)


def test_the_oldest_owed_timestamp_is_when_the_work_arrived(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)
    stuck = datetime.now(timezone.utc) - timedelta(hours=3)
    EventRecord.objects.update(recorded_at=stuck)
    assert outbox_health().oldest_owed_at == stuck


def test_a_failing_receiver_does_not_produce_a_negative_age(
    order: OrderPlaced, record: list[str]
) -> None:
    """The backoff pushes available_at into the future on every failed
    attempt, so an age read off that column goes negative exactly while a
    receiver is failing - which is when the alert has to fire."""
    with transaction.atomic():
        fire(order)
    recorded = EventRecord.objects.get().recorded_at
    DeliveryRecord.objects.update(
        status=DeliveryStatus.FAILED,
        attempts=2,
        available_at=datetime.now(timezone.utc) + timedelta(minutes=30),
    )

    oldest = outbox_health().oldest_owed_at
    assert oldest == recorded
    assert oldest <= datetime.now(timezone.utc), "the age must never be negative"


def test_a_settled_row_does_not_hold_the_oldest_timestamp_open(
    order: OrderPlaced, record: list[str]
) -> None:
    """Draining the queue must clear it, or the alert never resets."""
    with transaction.atomic():
        fire(order)
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(days=2))
    drain_outbox()
    assert outbox_health().oldest_owed_at is None


def test_dead_is_counted_but_not_owed(order: OrderPlaced, record: list[str]) -> None:
    """A dead letter is not work the relay will do. Counting it as owed would
    make a backlog alert fire forever after one bad deploy."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5)
    health = outbox_health()
    assert (health.owed, health.dead) == (0, 2)


def test_a_lapsed_lease_is_visible(order: OrderPlaced, record: list[str]) -> None:
    """Steady non-zero means workers are dying mid-delivery, or a receiver
    outruns its lease and has its work thrown away every time."""
    now = datetime.now(timezone.utc)
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1", lease_expires_at=now - timedelta(minutes=1)
    )
    assert outbox_health(now=now).lapsed_leases == 2


def test_a_live_lease_is_not_counted_as_lapsed(order: OrderPlaced, record: list[str]) -> None:
    now = datetime.now(timezone.utc)
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1", lease_expires_at=now + timedelta(hours=1)
    )
    assert outbox_health(now=now).lapsed_leases == 0


def test_the_per_receiver_split_names_who_is_behind(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.DEAD, attempts=5
    )
    by_key = {r.key: r for r in outbox_health().receivers}
    assert by_key["testapp.durable_receiver"].owed == 1
    assert by_key["testapp.durable_receiver"].dead == 0
    assert by_key["testapp.with_context"].owed == 0
    assert by_key["testapp.with_context"].dead == 1


def test_receivers_with_nothing_owed_or_dead_are_left_out(
    order: OrderPlaced, record: list[str]
) -> None:
    """A list naming every declared receiver every time is a list nobody
    reads."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    assert outbox_health().receivers == ()


def test_the_worst_backlog_comes_first(order: OrderPlaced, record: list[str]) -> None:
    """An operator scanning the output reads the top line first, so it has to
    be the receiver furthest behind."""
    with transaction.atomic():
        fire(order)
        fire(replace(order, order_id=8))
    # The receiver registered *second* is made the worst one, so the expected
    # order differs from the order the rows come back in. Starving the first
    # instead passes with no sort at all.
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").order_by(
        "pk"
    ).first().delete()

    entries = outbox_health().receivers
    assert [(e.key, e.owed) for e in entries] == [
        ("testapp.with_context", 2),
        ("testapp.durable_receiver", 1),
    ]


def test_a_receiver_backlog_carries_its_own_oldest(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
        fire(replace(order, order_id=9))
    stuck = datetime.now(timezone.utc) - timedelta(hours=5)
    oldest_event = EventRecord.objects.order_by("pk").first()
    EventRecord.objects.filter(pk=oldest_event.pk).update(recorded_at=stuck)
    # The other receiver is settled, so only one backlog is left to carry it.
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.SUCCEEDED
    )
    by_key = {r.key: r for r in outbox_health().receivers}
    assert by_key["testapp.durable_receiver"].oldest_owed_at == stuck
