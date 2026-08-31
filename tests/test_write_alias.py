from __future__ import annotations

import warnings

import pytest
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.write_alias import write_alias
from tests.conftest import receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True, databases=["default", "events"])


@pytest.fixture
def routed(settings):
    settings.DATABASE_ROUTERS = ["tests.testapp.routers.EventsRouter"]
    return settings


def test_the_alias_follows_the_router(routed) -> None:
    assert write_alias() == "events"


def test_the_warning_follows_the_rows_not_the_default_connection(
    routed, order: OrderPlaced, record: list[str]
) -> None:
    """Wrapping the wrong database is the dangerous case, and it was the silent
    one: the check asked ``default`` while every row went to ``events``, so it
    warned the caller who had it right and said nothing to the caller who did
    not."""
    with warnings.catch_warnings(record=True) as correct:
        warnings.simplefilter("always")
        with transaction.atomic(using="events"):
            fire(order)
    assert [str(w.message) for w in correct] == []

    with warnings.catch_warnings(record=True) as wrong:
        warnings.simplefilter("always")
        with transaction.atomic():  # default only; the event row is unprotected
            fire(order)
    assert any("outside a transaction" in str(w.message) for w in wrong)


def test_effectively_once_survives_a_router(routed, order: OrderPlaced, record: list[str]) -> None:
    """The headline guarantee: the receiver's work and the acknowledgement commit
    together. With the transaction opened on the wrong connection they did not,
    so a retry re-ran the side effect every time."""

    def write_then_explode(evt: OrderPlaced) -> None:
        EventRecord.objects.create(
            name="testapp.side_effect", version=1, payload={}, occurred_at=evt.placed_at
        )
        raise RuntimeError("after the write")

    with transaction.atomic(using="events"):
        fire(order)

    with receiver_replaced("testapp.durable_receiver", write_then_explode):
        for _ in range(3):
            drain_outbox()

    assert not EventRecord.objects.filter(name="testapp.side_effect").exists()


def test_rows_are_written_to_the_routed_database(
    routed, order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic(using="events"):
        fire(order)
    assert EventRecord.objects.using("events").count() == 1
    assert DeliveryRecord.objects.using("events").filter(status=DeliveryStatus.PENDING).exists()
