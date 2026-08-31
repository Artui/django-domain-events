from __future__ import annotations

import importlib
from unittest import mock

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


def test_limit_zero_requeues_nothing(order: OrderPlaced, record: list[str]) -> None:
    """An operator asking for the smallest possible blast radius must not get
    the largest one. Reading the limit for truthiness rather than for None turns
    'requeue at most nothing' into 'requeue everything'."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)

    assert requeue_dead(limit=0) == 0
    assert DeliveryRecord.objects.filter(status=DeliveryStatus.DEAD).count() == 2


def test_a_negative_limit_is_refused(order: OrderPlaced, record: list[str]) -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        requeue_dead(limit=-1)


def test_it_will_not_wipe_a_live_claim(order: OrderPlaced, record: list[str]) -> None:
    """The status guard on the update, not on the read.

    The interleaving has to be real: an earlier version of this test claimed the
    row *before* calling, so the select never picked it up and the test passed
    with the guard removed. The row must be DEAD when requeue chooses it and
    CLAIMED by the time requeue writes - so the steal is hooked onto the clock
    read that happens between those two statements.
    """
    module = importlib.import_module("django_domain_events.requeue_dead")

    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)
    stolen_id = DeliveryRecord.objects.order_by("pk").values_list("pk", flat=True).first()

    real_datetime = module.datetime

    class StealOnClockRead:
        @staticmethod
        def now(tz=None):
            DeliveryRecord.objects.filter(pk=stolen_id).update(
                status=DeliveryStatus.CLAIMED, claimed_by="relay-b"
            )
            return real_datetime.now(tz)

    with mock.patch.object(module, "datetime", StealOnClockRead):
        assert requeue_dead() == 1

    stolen = DeliveryRecord.objects.get(pk=stolen_id)
    assert (stolen.status, stolen.claimed_by) == (DeliveryStatus.CLAIMED, "relay-b")


def test_it_clears_the_dead_letter_message(order: OrderPlaced, record: list[str]) -> None:
    """A requeued row that later succeeds should not still show why it died."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, last_error="boom")
    requeue_dead()
    assert set(DeliveryRecord.objects.values_list("last_error", flat=True)) == {""}


def test_it_chunks_the_update(order: OrderPlaced, record: list[str], settings) -> None:
    """SQLite refuses more than 32,766 parameters in one statement, and a
    dead-letter table past that is an ordinary outcome of one bad deploy."""
    settings.DJANGO_DOMAIN_EVENTS = {"BATCH_SIZE": 1}
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)
    assert requeue_dead() == 2
