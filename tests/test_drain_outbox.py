"""Tests mirroring ``django_domain_events/drain_outbox.py``."""

from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
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
    from django_domain_events.registry import registry

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
