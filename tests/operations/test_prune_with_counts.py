from __future__ import annotations

import pytest
from django.utils import timezone as django_timezone

from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.prune_with_counts import prune_with_counts
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.pruned import Pruned

pytestmark = pytest.mark.django_db


def _consumed_with_deliveries(count: int) -> None:
    event = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        payload={},
        occurred_at=django_timezone.now(),
        delete_when="settled",
    )
    for index in range(count):
        DeliveryRecord.objects.create(
            event=event,
            receiver_key=f"tests.r{index}",
            status=DeliveryStatus.SUCCEEDED,
            available_at=django_timezone.now(),
            succeeded_at=django_timezone.now(),
        )


def test_it_counts_the_delivery_rows_a_batch_deletes() -> None:
    _consumed_with_deliveries(3)
    assert prune_with_counts() == Pruned(1, 3)


def test_it_counts_the_delivery_rows_deleted_a_chunk_at_a_time() -> None:
    """More delivery rows than ``batch_size``: they go in chunks before their event."""
    _consumed_with_deliveries(3)
    assert prune_with_counts(batch_size=2) == Pruned(1, 3)
