from __future__ import annotations

import importlib
from unittest import mock

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


def test_it_will_not_wipe_a_live_claim(order: OrderPlaced, record: list[str]) -> None:
    """The status predicate on the update, not only on the read.

    Interleaved for real: an earlier version claimed the row before calling, so
    the read already saw CLAIMED and the update never targeted it - it passed
    with the predicate removed. The membership test that decides what to reopen
    runs between the read and the write, so the steal is hooked there.
    """
    module = importlib.import_module("django_domain_events.replay_events")

    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()
    stolen_id = DeliveryRecord.objects.order_by("pk").values_list("pk", flat=True).first()

    real_terminal = module._TERMINAL

    class StealWhenAsked(tuple):
        def __contains__(self, item: object) -> bool:
            DeliveryRecord.objects.filter(pk=stolen_id).update(
                status=DeliveryStatus.CLAIMED, claimed_by="relay-b"
            )
            return tuple.__contains__(self, item)

    with mock.patch.object(module, "_TERMINAL", StealWhenAsked(real_terminal)):
        assert replay_events([event_id])["reopened"] == 1

    stolen = DeliveryRecord.objects.get(pk=stolen_id)
    assert (stolen.status, stolen.claimed_by) == (DeliveryStatus.CLAIMED, "relay-b")


def test_one_events_collision_does_not_discard_the_others(
    order: OrderPlaced, record: list[str]
) -> None:
    """One transaction for the whole call meant a conflict on any single event
    threw away the reopens for every other event the operator named."""
    with transaction.atomic():
        first = fire(order)
        second = fire(order)
    drain_outbox()

    counts = replay_events([first, second])
    assert counts["reopened"] == 4


def test_it_wakes_a_waiting_relay(order: OrderPlaced, record: list[str]) -> None:
    """Replay makes rows owed exactly as fire() does, so it has to reach the same
    low-latency path; otherwise replayed work sits until the next poll."""
    # Patched on the module object, not by dotted path: `__init__` re-exports
    # `replay_events`, so the package attribute of that name is the function and
    # `mock.patch("django_domain_events.replay_events.notify_relay")` walks into
    # the function rather than the module.
    module = importlib.import_module("django_domain_events.replay_events")
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()

    with mock.patch.object(module, "notify_relay") as notify:
        replay_events([event_id])
    assert notify.called


def test_it_does_not_wake_anything_when_nothing_changed(
    order: OrderPlaced, record: list[str]
) -> None:
    module = importlib.import_module("django_domain_events.replay_events")
    with transaction.atomic():
        event_id = fire(order)

    with mock.patch.object(module, "notify_relay") as notify:
        replay_events([event_id])
    assert not notify.called
