"""Tests mirroring ``django_domain_events/drain_outbox.py``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction

from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_runs_the_real_delivery_path(order: OrderPlaced, record: list[str]) -> None:
    """Deliberately not a task_always_eager equivalent: bypassing the transport
    hides both the serialisation boundary and the timing, which is how a suite
    passes while production breaks. This skips only the waiting."""
    with transaction.atomic():
        fire(order)
    record.clear()

    assert drain_outbox() == {DeliveryStatus.SUCCEEDED: 2}
    assert sorted(record) == ["context:testapp.OrderPlaced:1", "durable:7"]
    assert not DeliveryRecord.objects.filter(status=DeliveryStatus.PENDING).exists()


def test_the_payload_is_rebuilt_rather_than_reused(order: OrderPlaced, record: list[str]) -> None:
    """The receiver is handed an instance decoded from the row, which is what a
    worker in another process gets. A helper that passed the original object
    through would agree with a payload that cannot round-trip."""
    seen: list[OrderPlaced] = []
    from django_domain_events.declaration.registry import registry

    entry = registry.receiver_for_key("testapp.durable_receiver")
    original = entry.func
    object.__setattr__(entry, "func", seen.append)
    try:
        with transaction.atomic():
            fire(order)
        drain_outbox()
    finally:
        object.__setattr__(entry, "func", original)

    assert seen[0] == order
    assert seen[0] is not order


def test_a_limit_is_honoured(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    record.clear()
    assert drain_outbox(limit=1) == {DeliveryStatus.SUCCEEDED: 1}


def test_a_drain_leaves_a_row_not_yet_due_alone_until_told_the_time(
    order: OrderPlaced, record: list[str]
) -> None:
    """``drain_outbox`` skips the backoff and never the schedule: otherwise
    every test that makes an offer would also expire it. ``now=`` is how a test
    moves forward to the due time instead of waiting for it."""
    with transaction.atomic():
        fire(order)
    due = datetime.now(timezone.utc) + timedelta(hours=48)
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        available_at=due, due_at=due
    )
    record.clear()

    assert drain_outbox() == {DeliveryStatus.SUCCEEDED: 1}
    assert record == ["context:testapp.OrderPlaced:1"]

    assert drain_outbox(now=due - timedelta(seconds=1)) == {}
    assert drain_outbox(now=due) == {DeliveryStatus.SUCCEEDED: 1}
    assert record == ["context:testapp.OrderPlaced:1", "durable:7"]


def test_a_drain_told_the_time_still_skips_the_backoff(
    order: OrderPlaced, record: list[str]
) -> None:
    """``now=`` moves the clock the schedule is read against; it does not turn
    the backoff back on. A retry due long after ``now`` still runs."""
    with transaction.atomic():
        fire(order)
    now = datetime.now(timezone.utc)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.FAILED, attempts=1, available_at=now + timedelta(days=1)
    )

    assert drain_outbox(now=now + timedelta(minutes=1)) == {DeliveryStatus.SUCCEEDED: 2}


def test_respecting_the_backoff_reads_it_against_the_time_given(
    order: OrderPlaced, record: list[str]
) -> None:
    """With the backoff respected, ``now=`` is the moment it is respected at:
    a retry an hour away is left before the hour and run after it."""
    with transaction.atomic():
        fire(order)
    retry = datetime.now(timezone.utc) + timedelta(hours=1)
    DeliveryRecord.objects.update(status=DeliveryStatus.FAILED, attempts=1, available_at=retry)

    assert drain_outbox(respect_backoff=True) == {}
    assert drain_outbox(respect_backoff=True, now=retry) == {DeliveryStatus.SUCCEEDED: 2}
