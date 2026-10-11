from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext

from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered
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
    from django_domain_events.delivery.claim_batch import _locked
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


def _mail(lane: str = "mail") -> RegisteredReceiver:
    """A receiver of the same event as the testapp's two, declared in a lane."""
    return RegisteredReceiver(
        key="tests.mail",
        event_class=OrderPlaced,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane=lane,
    )


def _keys(ids: list[int]) -> set[str]:
    return set(DeliveryRecord.objects.filter(pk__in=ids).values_list("receiver_key", flat=True))


DEFAULT_LANE_KEYS = {"testapp.durable_receiver", "testapp.with_context"}


def test_a_named_lane_claims_only_its_own_receivers(order: OrderPlaced, record: list[str]) -> None:
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, lane="mail")

    assert _keys(ids) == {"tests.mail"}


def test_the_default_lane_claims_everything_no_named_lane_takes(
    order: OrderPlaced, record: list[str]
) -> None:
    """Which is what isolates a slow lane: the default relay has to stay out of
    it, and an exclusion list kept in deployment manifests goes stale."""
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, lane="default")

    assert _keys(ids) == DEFAULT_LANE_KEYS


def test_no_lane_claims_every_lane(order: OrderPlaced, record: list[str]) -> None:
    """What deliver_pending() and drain_outbox() have always done, and keep doing."""
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10)

    assert _keys(ids) == DEFAULT_LANE_KEYS | {"tests.mail"}


def test_a_lapsed_claim_in_a_named_lane_stays_out_of_the_default_lane(
    order: OrderPlaced, record: list[str]
) -> None:
    """The lane filter covers both arms of the claim. Applied to the retryable
    arm alone, the default relay would take over every row a crashed mail relay
    left behind."""
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        claim_batch(worker_id="dead", now=_now(), lease=timedelta(seconds=1), limit=10)
        later = _now() + timedelta(seconds=30)

        default = claim_batch(worker_id="w1", now=later, lease=LEASE, limit=10, lane="default")
        mail = claim_batch(worker_id="w2", now=later, lease=LEASE, limit=10, lane="mail")

    assert _keys(default) == DEFAULT_LANE_KEYS
    assert _keys(mail) == {"tests.mail"}


def test_a_deleted_receivers_rows_drain_through_the_default_lane(
    order: OrderPlaced, record: list[str]
) -> None:
    """A row whose receiver is gone belongs to no named lane, so the default
    relay claims it and records it orphaned. Phrasing the default lane as the
    receivers declared in it would strand that row forever."""
    with receiver_registered(_mail()), transaction.atomic():
        fire(order)

    ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, lane="default")

    assert _keys(ids) == DEFAULT_LANE_KEYS | {"tests.mail"}


def test_lane_membership_is_read_from_the_registry_at_claim_time(
    order: OrderPlaced, record: list[str]
) -> None:
    """Not copied onto the row, so moving a receiver to another lane moves the
    rows it is still owed with it."""
    with receiver_registered(_mail()), transaction.atomic():
        fire(order)
    with receiver_registered(_mail("search")):
        assert claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, lane="mail") == []
        ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, lane="search")

    assert _keys(ids) == {"tests.mail"}


def test_a_lane_with_no_receivers_claims_nothing(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)

    assert claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, lane="mail") == []


def test_without_named_lanes_the_default_lane_sends_the_unfiltered_claim() -> None:
    """Byte for byte, so a deployment that never declares a lane runs exactly
    the query it ran before lanes existed."""
    fixed = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)

    def sql(**kwargs: str) -> list[str]:
        with CaptureQueriesContext(connection) as captured:
            claim_batch(worker_id="w1", now=fixed, lease=LEASE, limit=10, **kwargs)
        return [q["sql"] for q in captured.captured_queries]

    assert sql(lane="default") == sql()
    assert sql(exclude_lanes=()) == sql(), "a relay with nothing paused changed its claim"


def test_a_paused_named_lane_is_left_out_of_a_claim_of_every_lane(
    order: OrderPlaced, record: list[str]
) -> None:
    """What lets a relay serving every lane keep the others flowing while one
    destination is throttling."""
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, exclude_lanes={"mail"})

    assert _keys(ids) == DEFAULT_LANE_KEYS


def test_a_paused_default_lane_leaves_only_the_named_lanes(
    order: OrderPlaced, record: list[str]
) -> None:
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        ids = claim_batch(
            worker_id="w1", now=_now(), lease=LEASE, limit=10, exclude_lanes={"default"}
        )

    assert _keys(ids) == {"tests.mail"}


def test_a_paused_default_lane_with_no_named_lanes_claims_nothing(
    order: OrderPlaced, record: list[str]
) -> None:
    """The default lane is *not the named ones*, so excluding it negates a
    negated ``__in`` over no values at all - which has to come out as nothing,
    not everything, on every backend."""
    with transaction.atomic():
        fire(order)

    assert (
        claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, exclude_lanes={"default"})
        == []
    )


def test_a_paused_lane_covers_its_lapsed_claims_too(order: OrderPlaced, record: list[str]) -> None:
    """The rows a relay hands back on a deferral are lapsed claims, and those
    are exactly the ones it must not take straight back."""
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        claim_batch(worker_id="dead", now=_now(), lease=timedelta(seconds=1), limit=10)
        later = _now() + timedelta(seconds=30)

        ids = claim_batch(worker_id="w1", now=later, lease=LEASE, limit=10, exclude_lanes={"mail"})

    assert _keys(ids) == DEFAULT_LANE_KEYS
