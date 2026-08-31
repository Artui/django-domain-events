from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.attributed import attributed, current_scope
from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.event_record import EventRecord
from tests.conftest import receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_the_scope_is_read_at_fire_time_not_at_commit(
    order: OrderPlaced, record: list[str]
) -> None:
    """The hard rule: capture eagerly, never read a ContextVar in deferred code.

    ``on_commit`` callbacks run at commit, which can be *after* the
    ``with attributed(...)`` block has exited - so a callback reading the
    variable then gets a stale answer or nothing, and does it silently.

    Firing inside the block and committing outside it is the shape that catches
    it, and the two assertions together are what make it a test rather than a
    demonstration: the scope really is gone by commit, and the row really does
    have the value. An implementation that read at commit could not satisfy
    both.
    """
    with transaction.atomic():
        with attributed(actor_key="system:inside"):
            fire(order)
        assert current_scope().actor_key == ""

    assert EventRecord.objects.get().actor_key == "system:inside"


def test_a_durable_delivery_reads_attribution_off_the_row(
    order: OrderPlaced, record: list[str]
) -> None:
    """A durable delivery can run in another process hours later. What it is
    told about the actor has to come off the row, because no context survives
    that distance."""
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
    """The mistake the rule exists to prevent, pinned so the difference between
    the row and the ambient scope stays visible: by delivery time the block is
    gone, and a receiver reaching for current_scope() finds it empty."""
    seen: list[str] = []

    with attributed(actor_key="system:firer"), transaction.atomic():
        fire(order)

    with receiver_replaced(
        "testapp.durable_receiver", lambda evt: seen.append(current_scope().actor_key)
    ):
        drain_outbox()

    assert seen == [""]
