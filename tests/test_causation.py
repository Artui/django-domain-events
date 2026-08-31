from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.attributed import attributed
from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.event_record import EventRecord
from tests.conftest import receiver_replaced
from tests.testapp.events import OrderPlaced, PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def test_an_event_fired_from_a_receiver_records_its_parent(
    order: OrderPlaced, record: list[str]
) -> None:
    """What lets the log answer what an event fanned out to, rather than only
    what caused it - and with no ceremony at the call site, because a parameter
    threaded through every receiver is a parameter someone forgets."""

    def fires_another(evt: OrderPlaced) -> None:
        fire(PinnedName(value=evt.order_id))

    with transaction.atomic():
        parent_id = fire(order)
    with receiver_replaced("testapp.durable_receiver", fires_another):
        drain_outbox()

    child = EventRecord.objects.get(name="testapp.pinned")
    assert child.causation_id == parent_id


def test_the_correlation_id_carries_down_the_chain(order: OrderPlaced, record: list[str]) -> None:
    """Causation is one hop; correlation is the whole tree. A child fired hours
    later in another process still belongs to the request that started it."""

    def fires_another(evt: OrderPlaced) -> None:
        fire(PinnedName(value=1))

    with attributed(source="checkout") as scope, transaction.atomic():
        fire(order)
    with receiver_replaced("testapp.durable_receiver", fires_another):
        drain_outbox()

    child = EventRecord.objects.get(name="testapp.pinned")
    assert child.correlation_id == scope.correlation_id


def test_a_root_event_has_no_cause(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    assert EventRecord.objects.get().causation_id is None


def test_causation_does_not_leak_past_the_delivery(order: OrderPlaced, record: list[str]) -> None:
    """The relay delivers many rows in a pass. A cause left set would attach the
    previous delivery's parent to whatever the next one fires."""
    with transaction.atomic():
        fire(order)
    drain_outbox()

    with transaction.atomic():
        fire(PinnedName(value=2))
    assert EventRecord.objects.get(name="testapp.pinned").causation_id is None
