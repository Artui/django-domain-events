from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from django.db import OperationalError, connection, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone as django_timezone

from django_domain_events.delivery import claim_batch as claim_batch_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.utils import GIVEN_BACK
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import in_project_time
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
    """The crash path: a worker that dies without acknowledging simply stops renewing."""
    with transaction.atomic():
        fire(order)
    claim_batch(worker_id="w1", now=_now(), lease=timedelta(seconds=1), limit=10)

    later = _now() + timedelta(seconds=30)
    reclaimed = claim_batch(worker_id="w2", now=later, lease=LEASE, limit=10)

    assert len(reclaimed) == 2
    assert set(DeliveryRecord.objects.values_list("claimed_by", flat=True)) == {"w2"}


def test_a_row_scheduled_for_later_is_not_claimed(order: OrderPlaced, record: list[str]) -> None:
    """Backoff is expressed as availability, so the claim query is what honours it."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(available_at=_now() + timedelta(hours=1))

    assert claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10) == []


def test_the_limit_is_honoured(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    assert len(claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=1)) == 1


def test_a_retry_due_now_waits_behind_every_row_due_before_it(
    order: OrderPlaced, record: list[str]
) -> None:
    """Backoff floors a row's queue place: the oldest available_at is claimed first."""
    with transaction.atomic():
        fire(order)
        fire(order)
    now = django_timezone.now()
    retry, *backlog = DeliveryRecord.objects.order_by("pk").values_list("pk", flat=True)
    DeliveryRecord.objects.filter(pk=retry).update(
        status=DeliveryStatus.FAILED, attempts=1, available_at=now - timedelta(seconds=1)
    )
    DeliveryRecord.objects.filter(pk__in=backlog).update(available_at=now - timedelta(hours=1))

    assert claim_batch(worker_id="w1", now=now, lease=LEASE, limit=len(backlog)) == backlog
    assert claim_batch(worker_id="w1", now=now, lease=LEASE, limit=10) == [retry]


def test_only_ids_narrows_the_claim(order: OrderPlaced, record: list[str]) -> None:
    """The eager path claims just the rows this process wrote, not the backlog."""
    with transaction.atomic():
        fire(order)
    everything = list(DeliveryRecord.objects.values_list("pk", flat=True))

    claimed = claim_batch(
        worker_id="eager", now=_now(), lease=LEASE, limit=10, only_ids=everything[:1]
    )
    assert claimed == everything[:1]


def test_it_locks_as_strongly_as_the_backend_allows() -> None:
    """Skipping spares workers a queue, and a blocking FOR UPDATE is still correct."""
    from django_domain_events.delivery.claim_batch import _locked
    from django_domain_events.models.delivery_record import DeliveryRecord

    base = DeliveryRecord.objects.all()

    # The decision the helper makes, not the SQL a backend compiles it to.
    def locking(**caps: bool) -> tuple[bool, bool]:
        query = _locked(base, **caps).query
        return query.select_for_update, query.select_for_update_skip_locked

    assert locking(skip_locked=True, for_update=True) == (True, True)
    assert locking(skip_locked=False, for_update=True) == (True, False)
    assert locking(skip_locked=False, for_update=False) == (False, False)


def test_a_scheduled_retry_can_be_claimed_when_backoff_is_ignored(
    order: OrderPlaced, record: list[str]
) -> None:
    """A suite cannot wait out the jittered interval a failure schedules."""
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
    """Isolates a slow lane: the default relay stays out, and manifests do not go stale."""
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
    """The lane filter covers both arms of the claim."""
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
    """A row whose receiver is gone is claimed by the default relay as orphaned."""
    with receiver_registered(_mail()), transaction.atomic():
        fire(order)

    ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, lane="default")

    assert _keys(ids) == DEFAULT_LANE_KEYS | {"tests.mail"}


def test_lane_membership_is_read_from_the_registry_at_claim_time(
    order: OrderPlaced, record: list[str]
) -> None:
    """Not copied onto the row, so moving a receiver moves the rows it is still owed."""
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
    """Byte for byte, so an undeclared-lane deployment runs the query it always ran."""
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
    """Lets an all-lanes relay keep others flowing while one destination throttles."""
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
    """Excluding the default lane negates an empty __in, which must yield nothing."""
    with transaction.atomic():
        fire(order)

    assert (
        claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, exclude_lanes={"default"})
        == []
    )


def test_a_paused_lane_covers_its_lapsed_claims_too(order: OrderPlaced, record: list[str]) -> None:
    """Deferred rows are lapsed claims, so the relay must not take them straight back."""
    with receiver_registered(_mail()):
        with transaction.atomic():
            fire(order)
        claim_batch(worker_id="dead", now=_now(), lease=timedelta(seconds=1), limit=10)
        later = _now() + timedelta(seconds=30)

        ids = claim_batch(worker_id="w1", now=later, lease=LEASE, limit=10, exclude_lanes={"mail"})

    assert _keys(ids) == DEFAULT_LANE_KEYS


def _schedule(key: str, due: datetime) -> int:
    """Hand-write a row due at ``due``, as ``fire()`` writes a scheduled one."""
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(receiver_key=key)
    DeliveryRecord.objects.filter(pk=delivery_id).update(available_at=due, due_at=due)
    return delivery_id


def test_a_scheduled_row_is_claimed_at_its_due_time_and_not_before(
    order: OrderPlaced, record: list[str]
) -> None:
    """A scheduled row's available_at is its due time, so backoff keeps it out."""
    with transaction.atomic():
        fire(order)
    due = _now() + timedelta(days=14)
    scheduled = _schedule("testapp.durable_receiver", due)

    early = claim_batch(worker_id="w1", now=due - timedelta(seconds=1), lease=LEASE, limit=10)
    assert scheduled not in early
    assert scheduled in claim_batch(worker_id="w1", now=due, lease=LEASE, limit=10)


def test_ignoring_the_backoff_never_ignores_the_schedule(
    order: OrderPlaced, record: list[str]
) -> None:
    """ignore_backoff claims a waiting retry, but never a row not yet due."""
    with transaction.atomic():
        fire(order)
    now = _now()
    scheduled = _schedule("testapp.durable_receiver", now + timedelta(hours=1))
    backing_off = DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.with_context"
    )
    DeliveryRecord.objects.filter(pk=backing_off).update(
        status=DeliveryStatus.FAILED,
        attempts=1,
        available_at=now + timedelta(hours=1),
        due_at=now - timedelta(minutes=5),
    )

    ids = claim_batch(worker_id="w1", now=now, lease=LEASE, limit=10, ignore_backoff=True)

    assert ids == [backing_off]
    assert scheduled not in ids


def test_ignoring_the_backoff_still_claims_a_row_with_no_due_time(
    order: OrderPlaced, record: list[str]
) -> None:
    """NULL due_at marks rows written before scheduling, which owe delivery now."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.FAILED, attempts=1, available_at=_now() + timedelta(hours=1)
    )

    ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10, ignore_backoff=True)

    assert len(ids) == 2


@pytest.mark.parametrize("ignore_backoff", [False, True], ids=["relay", "ignoring-backoff"])
def test_a_cancelled_row_is_never_claimed(
    order: OrderPlaced, record: list[str], ignore_backoff: bool
) -> None:
    """The claim names its statuses, so a cancelled row is never claimed."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.CANCELLED, available_at=_now())

    ids = claim_batch(
        worker_id="w1",
        now=_now() + timedelta(days=365),
        lease=LEASE,
        limit=10,
        ignore_backoff=ignore_backoff,
    )

    assert ids == []
    assert set(DeliveryRecord.objects.values_list("status", flat=True)) == {
        DeliveryStatus.CANCELLED
    }


def _dying(
    seen: list[DeliveryFailure], max_attempts: int = 3, site: str = "relay"
) -> RegisteredReceiver:
    """A receiver whose relay dies under it: its claims are left to lapse."""
    return RegisteredReceiver(
        key="tests.dying",
        event_class=OrderPlaced,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=max_attempts,
        eager=False,
        site=site,
        on_failure=seen.append,
    )


def _claims_left_to_lapse(pk: int, times: int, *, limit: int = 10) -> list[list[int]]:
    """Claim ``times`` times a minute apart under a one-second lease, never acknowledging."""
    start = _now()
    return [
        claim_batch(
            worker_id=f"w{n}",
            now=start + timedelta(minutes=n),
            lease=timedelta(seconds=1),
            limit=limit,
            only_ids=[pk],
        )
        for n in range(times)
    ]


def test_a_receiver_that_kills_its_relay_goes_dead_after_max_attempts_claims(
    order: OrderPlaced, record: list[str], caplog: pytest.LogCaptureFixture
) -> None:
    """The os._exit() receiver: three runs, each lapsed, and the fourth claim dead-letters it."""
    seen: list[DeliveryFailure] = []
    with receiver_registered(_dying(seen, max_attempts=3)):
        with transaction.atomic():
            fire(order)
        row = DeliveryRecord.objects.get(receiver_key="tests.dying")

        with caplog.at_level(logging.WARNING, logger="django_domain_events.delivery.deliver"):
            claims = _claims_left_to_lapse(row.pk, 4)

    row.refresh_from_db()
    assert claims == [[row.pk]] * 3 + [[]]
    assert (row.status, row.attempts) == (DeliveryStatus.DEAD, 3)
    assert row.completed_at is not None
    assert "lease" in row.last_error and "w2" in row.last_error
    assert f"at {row.claimed_at.isoformat()} and" in row.last_error
    assert f"ran out at {row.lease_expires_at.isoformat()}." in row.last_error
    assert "attempt 3 of 3." in row.last_error
    assert caplog.messages == [
        f"receiver tests.dying failed delivery {row.pk} on attempt 3 with a lapsed lease; "
        "the row is dead"
    ]
    assert [(f.status, f.attempt, f.delivery_id) for f in seen] == [
        (DeliveryStatus.DEAD, 3, row.pk)
    ]
    assert seen[0].error == row.last_error


@pytest.mark.parametrize("site", ["relay", "task"])
def test_a_lapsed_claim_spends_an_attempt_when_it_is_claimed_again(
    order: OrderPlaced, record: list[str], site: str
) -> None:
    """A task row whose message outwaited its lease counts too: the owner's call."""
    seen: list[DeliveryFailure] = []
    with receiver_registered(_dying(seen, site=site)):
        with transaction.atomic():
            fire(order)
        row = DeliveryRecord.objects.get(receiver_key="tests.dying")

        _claims_left_to_lapse(row.pk, 2)

    row.refresh_from_db()
    assert (row.status, row.attempts, row.claimed_by) == (DeliveryStatus.CLAIMED, 1, "w1")
    assert seen == []


def test_a_claim_then_success_records_one_attempt(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10)

    for pk in ids:
        dispatch_one(pk, worker_id="w1")

    assert set(DeliveryRecord.objects.values_list("status", "attempts")) == {
        (DeliveryStatus.SUCCEEDED, 1)
    }


def test_a_lapse_then_success_records_both_attempts(order: OrderPlaced, record: list[str]) -> None:
    """The relay that died spent one; the run that succeeded spent the second."""
    with transaction.atomic():
        fire(order)
    claim_batch(worker_id="dead", now=_now(), lease=timedelta(seconds=1), limit=10)
    later = _now() + timedelta(seconds=30)
    ids = claim_batch(worker_id="w1", now=later, lease=LEASE, limit=10)

    for pk in ids:
        dispatch_one(pk, worker_id="w1")

    assert set(DeliveryRecord.objects.values_list("status", "attempts")) == {
        (DeliveryStatus.SUCCEEDED, 2)
    }


def test_a_handed_back_claim_spends_no_attempt(order: OrderPlaced, record: list[str]) -> None:
    """A hand-back gives back rows nobody started, so claiming them again is not a lapse."""
    with transaction.atomic():
        fire(order)
    claimed_at = _now()
    ids = claim_batch(worker_id="w1", now=claimed_at, lease=LEASE, limit=10)
    assert hand_back(ids, worker_id="w1", claimed_at=claimed_at) == 2

    again = claim_batch(worker_id="w2", now=_now(), lease=LEASE, limit=10)

    assert sorted(again) == sorted(ids)
    assert set(DeliveryRecord.objects.values_list("attempts", flat=True)) == {0}


def test_a_claim_dead_lettering_lapses_still_fills_its_limit(
    order: OrderPlaced, record: list[str]
) -> None:
    """An empty claim reads as a drained queue, so the dead rows must not take its places."""
    seen: list[DeliveryFailure] = []
    with receiver_registered(_dying(seen, max_attempts=1)):
        with transaction.atomic():
            fire(order)
        dying = DeliveryRecord.objects.get(receiver_key="tests.dying")
        DeliveryRecord.objects.filter(pk=dying.pk).update(
            status=DeliveryStatus.CLAIMED,
            claimed_by="dead",
            claimed_at=_now() - timedelta(hours=1),
            lease_expires_at=_now() - timedelta(minutes=30),
            available_at=_now() - timedelta(hours=2),
        )

        ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=1)

    assert len(ids) == 1 and ids != [dying.pk]
    assert DeliveryRecord.objects.get(pk=dying.pk).status == DeliveryStatus.DEAD
    assert len(seen) == 1


def _lapse(pk: int) -> None:
    """Leave ``pk`` claimed by a dead worker whose lease ran out half an hour ago."""
    DeliveryRecord.objects.filter(pk=pk).update(
        status=DeliveryStatus.CLAIMED,
        claimed_by="dead",
        claimed_at=_now() - timedelta(hours=1),
        lease_expires_at=_now() - timedelta(minutes=30),
        available_at=_now() - timedelta(hours=2),
    )


def _one_lapse_at_its_last_attempt(order: OrderPlaced) -> int:
    """Two events' rows, of which the one dying row left is lapsed at its last attempt."""
    for _ in range(2):
        with transaction.atomic():
            fire(order)
    dying = list(DeliveryRecord.objects.filter(receiver_key="tests.dying").order_by("pk"))
    _lapse(dying[0].pk)
    DeliveryRecord.objects.filter(pk=dying[1].pk).delete()
    return dying[0].pk


def test_a_refill_claims_only_what_the_limit_has_left(
    order: OrderPlaced, record: list[str]
) -> None:
    """The first pass takes one row beside the dead one; the refill may take one more."""
    seen: list[DeliveryFailure] = []
    with receiver_registered(_dying(seen, max_attempts=1)):
        dead = _one_lapse_at_its_last_attempt(order)
        assert DeliveryRecord.objects.exclude(pk=dead).count() >= 3

        ids = claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=2)

    assert len(ids) == 2 and dead not in ids
    assert [f.delivery_id for f in seen] == [dead]


def test_on_failure_is_told_outside_the_claims_transaction(
    order: OrderPlaced, record: list[str]
) -> None:
    depths: list[int] = []
    entry = dataclasses.replace(
        _dying([], max_attempts=1),
        on_failure=lambda failure: depths.append(len(connection.atomic_blocks)),
    )
    with receiver_registered(entry):
        with transaction.atomic():
            fire(order)
        _lapse(DeliveryRecord.objects.get(receiver_key="tests.dying").pk)
        outer = len(connection.atomic_blocks)

        claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=10)

    assert depths == [outer]


def test_a_refill_that_raises_hands_back_its_rows_and_reports_its_dead(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first pass committed, so its rows are given back and its dead row reported."""
    seen: list[DeliveryFailure] = []
    real = claim_batch_module._claim_once
    passes: list[int] = []

    def second_pass_fails(*args: Any, **kwargs: Any) -> tuple[list[int], list[int]]:
        passes.append(kwargs["limit"])
        if len(passes) > 1:
            raise OperationalError("the second pass lost its connection")
        return real(*args, **kwargs)

    monkeypatch.setattr(claim_batch_module, "_claim_once", second_pass_fails)
    with receiver_registered(_dying(seen, max_attempts=1)):
        dead = _one_lapse_at_its_last_attempt(order)
        with pytest.raises(OperationalError, match="second pass"):
            claim_batch(worker_id="w1", now=_now(), lease=LEASE, limit=2)

    taken = DeliveryRecord.objects.filter(claimed_by="w1")
    assert passes == [2, 1]
    assert [f.delivery_id for f in seen] == [dead]
    assert taken.count() == 1
    assert set(taken.values_list("status", "lease_expires_at")) == {
        (DeliveryStatus.CLAIMED, in_project_time(GIVEN_BACK))
    }
