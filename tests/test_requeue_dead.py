from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.requeue_dead import requeue_dead
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_gives_dead_rows_their_budget_back(order: OrderPlaced, record: list[str]) -> None:
    """A row requeued at its limit dead-letters again on the first failure, and
    the operator learns nothing they did not already know."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5, claimed_by="w1")

    assert requeue_dead() == 2
    row = DeliveryRecord.objects.first()
    assert (row.status, row.attempts, row.claimed_by) == (DeliveryStatus.PENDING, 0, "")


def test_it_leaves_everything_else_alone(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    assert requeue_dead() == 0
    assert set(DeliveryRecord.objects.values_list("status", flat=True)) == {DeliveryStatus.PENDING}


def test_it_can_be_scoped_to_one_receiver(order: OrderPlaced, record: list[str]) -> None:
    """The usual reason to requeue is that one downstream was broken and now is
    not."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)

    assert requeue_dead(receiver_key="testapp.durable_receiver") == 1
    assert (
        DeliveryRecord.objects.values_list("status", flat=True).get(
            receiver_key="testapp.with_context"
        )
        == DeliveryStatus.DEAD
    )


def test_a_limit_caps_it(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)
    assert requeue_dead(limit=1) == 1
