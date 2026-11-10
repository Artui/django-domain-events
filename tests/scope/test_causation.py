from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.event_record import EventRecord
from django_domain_events.scope.attributed import attributed
from tests.conftest import receiver_replaced
from tests.testapp.events import OrderPlaced, PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def test_an_event_fired_from_a_receiver_records_its_parent(
    order: OrderPlaced, record: list[str]
) -> None:
    """The log answers what an event fanned out to, with no call-site ceremony."""

    def fires_another(evt: OrderPlaced) -> None:
        fire(PinnedName(value=evt.order_id))

    with transaction.atomic():
        parent_id = fire(order)
    with receiver_replaced("testapp.durable_receiver", fires_another):
        drain_outbox()

    child = EventRecord.objects.get(name="testapp.pinned")
    assert child.causation_id == parent_id


def test_the_correlation_id_carries_down_the_chain(order: OrderPlaced, record: list[str]) -> None:
    """Causation is one hop; correlation is the whole tree."""

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
    """The relay delivers many rows in a pass."""
    with transaction.atomic():
        fire(order)
    drain_outbox()

    with transaction.atomic():
        fire(PinnedName(value=2))
    assert EventRecord.objects.get(name="testapp.pinned").causation_id is None


def test_an_inline_receiver_records_the_parent(order: OrderPlaced, record: list[str]) -> None:
    """Causation is about descent, not about which execution site a receiver happens to use."""

    def fires_another(evt: OrderPlaced) -> None:
        fire(PinnedName(value=1))

    with receiver_replaced("testapp.inline_receiver", fires_another), transaction.atomic():
        parent = fire(order)

    assert EventRecord.objects.get(name="testapp.pinned").causation_id == parent


def test_an_on_commit_receiver_records_the_parent_and_the_chain(
    order: OrderPlaced, record: list[str]
) -> None:
    """The cause is captured at fire time, since commit runs after the block exits."""

    def fires_another(evt: OrderPlaced) -> None:
        # An on-commit receiver runs after commit, in autocommit, and warns rightly.
        with transaction.atomic():
            fire(PinnedName(value=1))

    with receiver_replaced("testapp.on_commit_receiver", fires_another), transaction.atomic():  # noqa: SIM117 - the nesting is what this asserts
        with attributed(source="web") as scope:
            parent = fire(order)

    child = EventRecord.objects.get(name="testapp.pinned")
    assert (child.causation_id, child.correlation_id) == (parent, scope.correlation_id)


def test_a_block_inside_a_receiver_stays_in_the_chain(
    order: OrderPlaced, record: list[str]
) -> None:
    """Minting a fresh chain in a receiver detaches its fires from their cause."""

    def fires_another(evt: OrderPlaced) -> None:
        with attributed(actor_key="system:fulfilment"):
            fire(PinnedName(value=1))

    with attributed(source="checkout") as scope, transaction.atomic():
        fire(order)
    with receiver_replaced("testapp.durable_receiver", fires_another):
        drain_outbox()

    child = EventRecord.objects.get(name="testapp.pinned")
    assert child.correlation_id == scope.correlation_id
    assert child.actor_key == "system:fulfilment"
