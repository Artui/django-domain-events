from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.event_record import EventRecord
from django_domain_events.scope.attributed import attributed, current_scope
from tests.conftest import receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_the_scope_is_read_at_fire_time_not_at_commit(
    order: OrderPlaced, record: list[str]
) -> None:
    """The hard rule: capture eagerly, never read a ContextVar in deferred code."""
    with transaction.atomic():
        with attributed(actor_key="system:inside"):
            fire(order)
        assert current_scope().actor.key == ""

    assert EventRecord.objects.get().actor_key == "system:inside"


def test_a_durable_delivery_reads_attribution_off_the_row(
    order: OrderPlaced, record: list[str]
) -> None:
    """A durable delivery can run in another process hours later."""
    seen: list[str] = []

    with attributed(actor_key="system:firer"), transaction.atomic():
        fire(order)

    def reads_context(evt: OrderPlaced, ctx) -> None:
        seen.append(ctx.actor_key)

    with receiver_replaced("testapp.with_context", reads_context):
        drain_outbox()

    assert seen == ["system:firer"]


def test_a_receiver_reading_the_live_scope_gets_nothing(
    order: OrderPlaced, record: list[str]
) -> None:
    """By delivery time the ambient scope block is gone, so current_scope() is empty."""
    seen: list[str] = []

    with attributed(actor_key="system:firer"), transaction.atomic():
        fire(order)

    with receiver_replaced(
        "testapp.durable_receiver", lambda evt: seen.append(current_scope().actor.key)
    ):
        drain_outbox()

    assert seen == [""]
