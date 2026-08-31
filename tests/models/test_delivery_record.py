"""Tests mirroring ``django_domain_events/models/delivery_record.py``."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from django.db import IntegrityError, transaction

from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord

pytestmark = pytest.mark.django_db


def _event() -> EventRecord:
    return EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
    )


def test_it_identifies_the_pair_it_represents() -> None:
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    assert str(row) == f"testapp.durable_receiver <- event {event.pk} (pending)"


def test_one_delivery_per_event_and_receiver() -> None:
    """The unique constraint is the delivered-log the at-least-once promise owes
    people, rather than a separate table."""
    event = _event()
    DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    with pytest.raises(IntegrityError), transaction.atomic():
        DeliveryRecord.objects.create(
            event=event,
            receiver_key="testapp.durable_receiver",
            available_at=event.recorded_at,
        )


def test_deleting_an_event_takes_its_deliveries() -> None:
    """Cascade, so retention stays a single delete rather than an ordering
    problem."""
    event = _event()
    DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    event.delete()
    assert DeliveryRecord.objects.count() == 0
