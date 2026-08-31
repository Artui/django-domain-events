from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command
from django.db import transaction

from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_reports_what_it_requeued(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)

    out = StringIO()
    call_command("requeue_dead", stdout=out)
    assert "requeued: 2" in out.getvalue()


def test_it_can_be_scoped_and_capped(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)

    out = StringIO()
    call_command(
        "requeue_dead", "--receiver", "testapp.durable_receiver", "--limit", "5", stdout=out
    )
    assert "requeued: 1" in out.getvalue()
