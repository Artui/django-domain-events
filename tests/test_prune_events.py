from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.prune_events import prune_events
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced, PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def _age(days: int) -> None:
    """Backdate every event, since recorded_at is auto_now_add."""
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(days=days))


def test_it_deletes_settled_events_past_the_window(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    _age(120)

    assert prune_events() == 1
    assert not EventRecord.objects.exists()
    assert not DeliveryRecord.objects.exists()


def test_it_never_deletes_an_event_that_is_still_owed(
    order: OrderPlaced, record: list[str]
) -> None:
    """The rule the whole thing turns on. Deleting a row with work outstanding
    drops an obligation nobody recorded as lost, which is the exact failure the
    outbox exists to prevent."""
    with transaction.atomic():
        fire(order)
    _age(365)

    assert prune_events() == 0
    assert EventRecord.objects.exists()


def test_a_delivery_in_flight_protects_its_event(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.CLAIMED
    )
    _age(365)

    assert prune_events() == 0


def test_a_dead_delivery_is_settled(order: OrderPlaced, record: list[str]) -> None:
    """Dead means the package has stopped trying. Holding the event forever
    would make a permanent failure a permanent leak."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)
    _age(365)

    assert prune_events() == 1


def test_an_event_with_no_deliveries_is_settled(record: list[str]) -> None:
    """A suppressed event, or one with no durable receivers, is settled by
    definition - there is nothing that could still be owed."""
    with transaction.atomic():
        fire(PinnedName(value=1))
    _age(365)

    assert prune_events() == 1


def test_events_inside_the_window_are_kept(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    _age(10)

    assert prune_events(timedelta(days=90)) == 0
    assert prune_events(timedelta(days=1)) == 1


def test_it_deletes_in_batches(order: OrderPlaced, record: list[str]) -> None:
    """A single statement over a year of rows holds a lock for as long as it
    runs, on the table the relay is trying to claim from."""
    with transaction.atomic():
        for _ in range(7):
            fire(PinnedName(value=1))
    _age(365)

    assert prune_events(batch_size=2) == 7


def test_a_limit_stops_it_early(record: list[str]) -> None:
    with transaction.atomic():
        for _ in range(7):
            fire(PinnedName(value=1))
    _age(365)

    assert prune_events(limit=3) == 3
    assert EventRecord.objects.count() == 4


def test_a_replay_between_the_select_and_the_delete_is_respected(
    order: OrderPlaced, record: list[str]
) -> None:
    """Settledness is re-checked at the delete. A replay landing in between makes
    rows owed again, and the cascade would take them with no record that anything
    was lost - after the operator had been told they were reopened."""
    from django_domain_events.replay_events import replay_events

    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()
    _age(365)

    # The interleaving, made deterministic: the event is settled when prune
    # chooses it and owed again by the time prune writes.
    assert replay_events([event_id])["reopened"] == 2
    assert prune_events() == 0
    assert EventRecord.objects.filter(pk=event_id).exists()


def test_it_counts_events_not_cascaded_rows(order: OrderPlaced, record: list[str]) -> None:
    """delete() reports every object it removed, including the delivery rows the
    cascade takes - so a caller asking how many events went would be told how
    many rows went."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    _age(365)

    assert prune_events() == 1
