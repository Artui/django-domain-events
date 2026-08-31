from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction

from django_domain_events.claim_batch import claim_batch
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

LEASE = timedelta(seconds=300)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_claiming_marks_the_rows_and_returns_their_ids(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)

    ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10)

    assert len(ids) == 2
    claimed = DeliveryRecord.objects.filter(pk__in=ids)
    assert {row.status for row in claimed} == {DeliveryStatus.CLAIMED}
    assert {row.claimed_by for row in claimed} == {"w1"}
    assert all(row.lease_expires_at > row.claimed_at for row in claimed)


def test_a_claimed_row_is_not_claimed_again(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10)

    assert claim_batch(worker_id="w2", now=_now(), lease=LEASE, limit=10) == []


def test_a_lapsed_lease_becomes_claimable_again(order: OrderPlaced, record: list[str]) -> None:
    """The crash path, and it is the same path as an ordinary retry rather than
    a special case: a worker that dies without acknowledging simply stops
    renewing."""
    with transaction.atomic():
        fire(order)
    claim_batch(worker_id="w1", now=_now(), lease=timedelta(seconds=1), limit=10)

    later = _now() + timedelta(seconds=30)
    reclaimed = claim_batch(worker_id="w2", now=later, lease=LEASE, limit=10)

    assert len(reclaimed) == 2
    assert set(DeliveryRecord.objects.values_list("claimed_by", flat=True)) == {"w2"}


def test_a_row_scheduled_for_later_is_not_claimed(order: OrderPlaced, record: list[str]) -> None:
    """Backoff is expressed as availability, so the claim query is what honours
    it; nothing else has to remember that a row is serving a wait."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(available_at=_now() + timedelta(hours=1))

    assert claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10) == []


def test_the_limit_is_honoured(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    assert len(claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=1)) == 1


def test_only_ids_narrows_the_claim(order: OrderPlaced, record: list[str]) -> None:
    """What the eager path needs: claim the rows this process just wrote, and
    leave the rest of the backlog to the relay."""
    with transaction.atomic():
        fire(order)
    everything = list(DeliveryRecord.objects.values_list("pk", flat=True))

    claimed = claim_batch(
        worker_id="eager", now=_now(), lease=LEASE, limit=10, only_ids=everything[:1]
    )
    assert claimed == everything[:1]


def test_it_locks_as_strongly_as_the_backend_allows() -> None:
    """Skipping is preferred so workers do not queue behind each other, but a
    blocking FOR UPDATE is still correct - it serialises the claim rather than
    losing it. Only a backend with no row locking at all falls through, and that
    is what the relay refuses to start on."""
    from django_domain_events.claim_batch import _locked
    from django_domain_events.models.delivery_record import DeliveryRecord

    base = DeliveryRecord.objects.all()

    # The decision the helper makes, not the SQL a particular backend compiles
    # it to: SQLite drops the clause entirely, so reading the query string would
    # make this assert the backend rather than the code.
    def locking(**caps: bool) -> tuple[bool, bool]:
        query = _locked(base, **caps).query
        return query.select_for_update, query.select_for_update_skip_locked

    assert locking(skip_locked=True, for_update=True) == (True, True)
    assert locking(skip_locked=False, for_update=True) == (True, False)
    assert locking(skip_locked=False, for_update=False) == (False, False)


def test_a_scheduled_retry_can_be_claimed_when_backoff_is_ignored(
    order: OrderPlaced, record: list[str]
) -> None:
    """What the test helper needs: a failed delivery is scheduled a jittered
    interval ahead, and a suite cannot wait it out."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.FAILED, available_at=_now() + timedelta(hours=1)
    )

    assert claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10) == []
    assert (
        len(claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, ignore_backoff=True))
        == 2
    )
