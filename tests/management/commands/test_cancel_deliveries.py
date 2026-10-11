from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import transaction

from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_reports_what_it_cancelled_and_what_was_in_flight(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        event_id = fire(order)
    claimed = DeliveryRecord.objects.order_by("pk").first()
    DeliveryRecord.objects.filter(pk=claimed.pk).update(status=DeliveryStatus.CLAIMED)

    out = StringIO()
    call_command("cancel_deliveries", str(event_id), stdout=out)

    assert "cancelled: 1, in_flight: 1" in out.getvalue()
    assert DeliveryRecord.objects.filter(status=DeliveryStatus.CANCELLED).count() == 1
    assert DeliveryRecord.objects.get(pk=claimed.pk).status == DeliveryStatus.CLAIMED


def test_it_can_be_narrowed_to_one_receiver(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(order)

    out = StringIO()
    call_command(
        "cancel_deliveries", str(event_id), "--receiver", "testapp.durable_receiver", stdout=out
    )

    assert "cancelled: 1, in_flight: 0" in out.getvalue()
    assert list(
        DeliveryRecord.objects.filter(status=DeliveryStatus.CANCELLED).values_list(
            "receiver_key", flat=True
        )
    ) == ["testapp.durable_receiver"]


def test_it_refuses_a_call_naming_no_events(order: OrderPlaced, record: list[str]) -> None:
    """A cancel with no scope is refused rather than read as "every event":
    the smallest call an operator can type must not be the widest."""
    with transaction.atomic():
        fire(order)

    with pytest.raises(CommandError, match="event_ids"):
        call_command("cancel_deliveries")

    assert not DeliveryRecord.objects.filter(status=DeliveryStatus.CANCELLED).exists()
