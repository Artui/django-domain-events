"""Tests mirroring ``django_domain_events/delivery/hand_back.py``.

Each condition the hand-back is filtered on has a test here that fails when it
is deleted, because together they are one branch arc and coverage cannot see a
missing one.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction

from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.introspection.outbox_health import outbox_health
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

LEASE = timedelta(minutes=5)


def _claimed(order: OrderPlaced, worker_id: str = "w1") -> tuple[list[int], datetime]:
    """Fire one event and claim its rows, returning the ids and the claim time."""
    with transaction.atomic():
        fire(order)
    claimed_at = datetime.now(timezone.utc)
    ids = claim_batch(worker_id=worker_id, now=claimed_at, lease=LEASE, limit=10)
    assert len(ids) == 2
    return ids, claimed_at


def _claimable_at(moment: datetime) -> list[int]:
    return claim_batch(worker_id="w2", now=moment, lease=LEASE, limit=10)


def test_handed_back_rows_are_claimable_at_once(order: OrderPlaced, record: list[str]) -> None:
    """At the very moment they were claimed, even: the claim's lapsed-lease arm
    is a strict comparison, so a lease ended "now" would still be held by a
    claim made in the same instant, on a frozen clock or on a worker whose
    clock is a little behind."""
    ids, claimed_at = _claimed(order)
    assert _claimable_at(claimed_at) == []

    assert hand_back(ids, worker_id="w1", claimed_at=claimed_at) == 2

    assert sorted(_claimable_at(claimed_at)) == sorted(ids)


def test_a_handed_back_row_keeps_its_status_and_place(
    order: OrderPlaced, record: list[str]
) -> None:
    """Nothing to restore: the row stays CLAIMED and keeps its ``available_at``,
    so it is still at the head of the queue."""
    ids, claimed_at = _claimed(order)
    before = dict(DeliveryRecord.objects.values_list("pk", "available_at"))

    hand_back(ids, worker_id="w1", claimed_at=claimed_at)

    assert set(DeliveryRecord.objects.values_list("status", flat=True)) == {DeliveryStatus.CLAIMED}
    assert dict(DeliveryRecord.objects.values_list("pk", "available_at")) == before


def test_handed_back_rows_count_as_lapsed_until_reclaimed(
    order: OrderPlaced, record: list[str]
) -> None:
    """The operations page says so, because a stop shows up on a health
    dashboard as lapsed leases for the moment it takes another relay to claim."""
    ids, claimed_at = _claimed(order)
    assert outbox_health().lapsed_leases == 0

    hand_back(ids, worker_id="w1", claimed_at=claimed_at)
    assert outbox_health().lapsed_leases == 2

    _claimable_at(datetime.now(timezone.utc))
    assert outbox_health().lapsed_leases == 0


def test_only_the_named_rows_are_handed_back(order: OrderPlaced, record: list[str]) -> None:
    """Holds the ids: a row the worker has started - one handed to a task
    backend - is CLAIMED under the same claim, and must keep its lease."""
    ids, claimed_at = _claimed(order)

    assert hand_back(ids[:1], worker_id="w1", claimed_at=claimed_at) == 1

    assert _claimable_at(claimed_at) == ids[:1]


def test_a_row_another_worker_has_claimed_is_not_touched(
    order: OrderPlaced, record: list[str]
) -> None:
    """Holds ``claimed_by``. Built at the same instant on purpose, so the claim
    time alone cannot tell the two claims apart."""
    ids, claimed_at = _claimed(order, worker_id="someone-else")

    assert hand_back(ids, worker_id="w1", claimed_at=claimed_at) == 0

    assert _claimable_at(claimed_at) == []


def test_a_later_claim_by_the_same_worker_id_is_not_touched(
    order: OrderPlaced, record: list[str]
) -> None:
    """Holds ``claimed_at``. A fixed ``--worker-id`` survives a restart, so the
    id alone cannot tell this process's claim from the one before it: the rows
    a crashed predecessor claimed, and this process reclaimed when the lease
    lapsed, belong to the new claim and not to the old one."""
    ids, old_claim = _claimed(order, worker_id="box-1")
    new_claim = old_claim + LEASE + timedelta(seconds=1)
    assert sorted(claim_batch(worker_id="box-1", now=new_claim, lease=LEASE, limit=10)) == sorted(
        ids
    )

    assert hand_back(ids, worker_id="box-1", claimed_at=old_claim) == 0

    assert _claimable_at(new_claim) == []


def test_a_settled_row_is_not_touched(order: OrderPlaced, record: list[str]) -> None:
    """Holds the status. A row this worker has finished still carries its
    claim, and a hand-back counting it would report rows it never gave back."""
    ids, claimed_at = _claimed(order)
    DeliveryRecord.objects.filter(pk=ids[0]).update(status=DeliveryStatus.SUCCEEDED)
    lease = DeliveryRecord.objects.get(pk=ids[0]).lease_expires_at

    assert hand_back(ids, worker_id="w1", claimed_at=claimed_at) == 1

    assert DeliveryRecord.objects.get(pk=ids[0]).lease_expires_at == lease


def test_an_empty_hand_back_is_a_no_op() -> None:
    assert hand_back([], worker_id="w1", claimed_at=datetime.now(timezone.utc)) == 0
