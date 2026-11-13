from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timezone

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_domain_events.introspection.delivery_tally import delivery_tally
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.delivery_tally import DeliveryTally

pytestmark = pytest.mark.django_db

OUTCOMES = [f.name for f in fields(DeliveryTally) if f.name != "total"]


def _event() -> EventRecord:
    return EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )


def _deliver(event: EventRecord, status: str, n: int, tag: str = "") -> None:
    for i in range(n):
        DeliveryRecord.objects.create(
            event=event,
            receiver_key=f"probe.{status}{tag}.{i}",
            status=status,
            available_at=event.recorded_at,
        )


def test_counts_match_the_rows_of_an_event_with_mixed_outcomes() -> None:
    event, other = _event(), _event()
    for status, n in [
        (DeliveryStatus.PENDING, 2),
        (DeliveryStatus.FAILED, 1),
        (DeliveryStatus.CLAIMED, 1),
        (DeliveryStatus.SUCCEEDED, 3),
        (DeliveryStatus.CANCELLED, 2),
        (DeliveryStatus.DEAD, 1),
        (DeliveryStatus.ORPHANED, 2),
    ]:
        _deliver(event, status, n)
    _deliver(other, DeliveryStatus.SUCCEEDED, 5)

    assert delivery_tally(event.pk) == DeliveryTally(
        owed=4, succeeded=3, cancelled=2, dead=1, orphaned=2, total=12
    )


def test_a_moot_delivery_counts_as_cancelled() -> None:
    event = _event()
    _deliver(event, DeliveryStatus.CANCELLED, 1)
    DeliveryRecord.objects.update(last_error="moot: token gone")
    assert delivery_tally(event.pk).cancelled == 1


def test_one_query_per_call() -> None:
    event = _event()
    _deliver(event, DeliveryStatus.PENDING, 2)
    _deliver(event, DeliveryStatus.DEAD, 1)
    with CaptureQueriesContext(connection) as queries:
        delivery_tally(event.pk)
    assert len(queries) == 1
    assert "GROUP BY" in queries[0]["sql"].upper()


def test_an_unknown_event_is_all_zeros() -> None:
    assert delivery_tally(999_999) == DeliveryTally(0, 0, 0, 0, 0, 0)


def test_the_tally_is_frozen() -> None:
    with pytest.raises(FrozenInstanceError):
        delivery_tally(1).owed = 1


@pytest.mark.parametrize("status", list(DeliveryStatus))
def test_every_status_lands_in_exactly_one_field(status: DeliveryStatus) -> None:
    event = _event()
    _deliver(event, status, 1)
    tally = delivery_tally(event.pk)
    assert tally.total == 1
    assert sum(getattr(tally, name) for name in OUTCOMES) == tally.total
    assert sum(1 for name in OUTCOMES if getattr(tally, name)) == 1
