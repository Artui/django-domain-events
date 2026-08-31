from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.replay_events import replay_events
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import receiver_deleted
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def _status(key: str) -> str:
    return DeliveryRecord.objects.values_list("status", flat=True).get(receiver_key=key)


def test_a_delivered_event_can_be_made_owed_again(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()
    record.clear()

    assert replay_events([event_id]) == {"reopened": 2, "added": 0}
    assert _status("testapp.durable_receiver") == DeliveryStatus.PENDING

    drain_outbox()
    assert "durable:7" in record


def test_reopening_clears_the_previous_attempt(order: OrderPlaced, record: list[str]) -> None:
    """A row carrying its old attempt count and error would dead-letter on the
    first failure and tell an operator nothing new."""
    with transaction.atomic():
        event_id = fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.DEAD, attempts=5, last_error="boom", claimed_by="w1"
    )

    replay_events([event_id])
    row = DeliveryRecord.objects.first()
    assert (row.attempts, row.last_error, row.claimed_by) == (0, "", "")


def test_a_receiver_registered_later_can_be_given_the_backlog(
    order: OrderPlaced, record: list[str]
) -> None:
    """The other half of freezing the receiver set at fire time: a deploy does
    not hand a new receiver a week of events, and this is how you choose to."""
    with receiver_deleted("testapp.with_context"):
        with transaction.atomic():
            event_id = fire(order)
        assert DeliveryRecord.objects.count() == 1

    assert replay_events([event_id]) == {"reopened": 0, "added": 1}
    assert _status("testapp.with_context") == DeliveryStatus.PENDING


def test_a_delivery_in_flight_is_left_alone(order: OrderPlaced, record: list[str]) -> None:
    """Reopening a claimed row would hand the same work to two receivers, which
    is the one thing the lease exists to prevent."""
    with transaction.atomic():
        event_id = fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.CLAIMED)

    assert replay_events([event_id]) == {"reopened": 0, "added": 0}


def test_it_can_be_narrowed_to_one_receiver(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()

    replay_events([event_id], receiver_keys=["testapp.durable_receiver"])
    assert _status("testapp.durable_receiver") == DeliveryStatus.PENDING
    assert _status("testapp.with_context") == DeliveryStatus.SUCCEEDED


def test_an_event_whose_class_is_gone_is_skipped(order: OrderPlaced, record: list[str]) -> None:
    """Nothing can be replayed for a name the registry no longer has, and
    failing the whole batch over one of them helps nobody."""
    with transaction.atomic():
        event_id = fire(order)
    from django_domain_events.models.event_record import EventRecord

    EventRecord.objects.filter(pk=event_id).update(name="testapp.Retired")

    assert replay_events([event_id]) == {"reopened": 0, "added": 0}
