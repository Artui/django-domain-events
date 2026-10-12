"""A project on ``USE_TZ = False``, through the public surface.

Django stores and returns naive datetimes in ``TIME_ZONE`` local time under that
setting, and refuses an aware one on SQLite. The package used to take "now" as
an aware UTC value, so every comparison with a column it read back raised
``TypeError``, and every write of one failed on SQLite. Each test here switches
the setting itself, so it runs in the default suite on either backend - the
whole suite also runs under the setting in CI (``DDE_TEST_USE_TZ=false``), which
is the broad gate; these are the cases that name what broke.

The test settings leave ``TIME_ZONE`` at Django's America/Chicago, so a naive
value read as UTC would be hours out, not equal by accident.
"""

from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import pytest
from django.core.management import call_command
from django.db import transaction
from django.utils import timezone as django_timezone

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_one
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.not_yet import NotYet
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.introspection.outbox_health import outbox_health
from django_domain_events.introspection.quiet_receivers import quiet_receivers
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.prune_events import prune_events
from django_domain_events.operations.replay_events import replay_events
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def _naive(settings) -> None:
    settings.USE_TZ = False


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _fire() -> int:
    with transaction.atomic():
        event_id = fire(Unheard(value=1))
    assert event_id is not None
    return event_id


def _row(key: str) -> DeliveryRecord:
    return DeliveryRecord.objects.select_related("event").get(receiver_key=key)


def test_what_the_package_writes_reads_back_naive() -> None:
    """The writes follow the setting: SQLite refuses an aware value outright
    under it, and on Postgres an aware one would read back as a different
    kind of value from the one written."""
    receiver(Unheard, key="probe.ok")(lambda event: None)
    _fire()
    drain_outbox()
    row = _row("probe.ok")
    assert row.status == DeliveryStatus.SUCCEEDED
    for value in (row.available_at, row.completed_at, row.succeeded_at, row.event.recorded_at):
        assert django_timezone.is_naive(value)
    # Local time, not UTC: within a minute of the project's own clock.
    assert abs(row.succeeded_at - django_timezone.now()) < timedelta(minutes=1)


def test_a_deferral_that_does_not_count_is_recorded() -> None:
    """The give-up bound subtracted an aware now from a naive recorded_at, so
    every ``RetryAfter(counts=False)`` raised ``TypeError`` instead of being
    recorded."""

    def deferring(event: Unheard) -> None:
        raise RetryAfter(30, counts=False)

    receiver(Unheard, key="probe.deferring", give_up_after=timedelta(hours=1))(deferring)
    _fire()
    drain_outbox()
    row = _row("probe.deferring")
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 0)
    assert row.available_at > django_timezone.now()


def test_a_failed_row_run_directly_is_refused_until_its_backoff_ends() -> None:
    """The owed check compared an aware now with the naive ``available_at``, so
    a FAILED row handed to ``deliver_one`` without a claim raised."""
    receiver(Unheard, key="probe.backoff")(lambda event: None)
    _fire()
    DeliveryRecord.objects.filter(receiver_key="probe.backoff").update(
        status=DeliveryStatus.FAILED, available_at=django_timezone.now() + timedelta(hours=1)
    )
    assert deliver_one(_row("probe.backoff").pk) is None
    DeliveryRecord.objects.filter(receiver_key="probe.backoff").update(
        available_at=django_timezone.now() - timedelta(seconds=1)
    )
    assert deliver_one(_row("probe.backoff").pk) is DeliveryStatus.SUCCEEDED


def test_a_scheduled_receiver_is_written_at_its_due_time() -> None:
    receiver(Unheard, key="probe.later", due=timedelta(hours=2))(lambda event: None)
    _fire()
    row = _row("probe.later")
    assert row.due_at == row.event.occurred_at + timedelta(hours=2)
    assert django_timezone.is_naive(row.due_at)


def test_a_due_callable_may_answer_in_local_time() -> None:
    """Under this setting a naive value is the project's own convention, not a
    guess, so it is read as ``TIME_ZONE`` local time rather than refused."""
    when = django_timezone.now() + timedelta(days=1)
    receiver(Unheard, key="probe.local", due=lambda event: when)(lambda event: None)
    _fire()
    assert _row("probe.local").due_at == when


def test_a_due_callable_answering_an_aware_time_is_stored_as_local_time() -> None:
    when = datetime.now(timezone.utc) + timedelta(days=1)
    receiver(Unheard, key="probe.aware", due=lambda event: when)(lambda event: None)
    _fire()
    assert _row("probe.aware").due_at == django_timezone.make_naive(when)


def test_fire_stores_an_aware_occurred_at_as_local_time() -> None:
    occurred = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    with transaction.atomic():
        pk = fire(Unheard(value=1), occurred_at=occurred)
    assert EventRecord.objects.get(pk=pk).occurred_at == django_timezone.make_naive(occurred)


def test_not_yet_takes_local_time_and_moves_the_row_there() -> None:
    later = django_timezone.now() + timedelta(hours=3)

    def early(event: Unheard) -> None:
        raise NotYet(later)

    receiver(Unheard, key="probe.notyet")(early)
    _fire()
    drain_outbox()
    row = _row("probe.notyet")
    assert (row.status, row.available_at, row.due_at) == (DeliveryStatus.PENDING, later, later)


def test_not_yet_takes_an_aware_time_and_moves_the_row_to_its_local_time() -> None:
    later = datetime.now(timezone.utc) + timedelta(hours=3)
    assert NotYet(later).until == django_timezone.make_naive(later)


def test_not_yet_still_refuses_a_time_already_past() -> None:
    with pytest.raises(ValueError, match="in the future"):
        NotYet(django_timezone.now() - timedelta(minutes=1))


def test_health_ages_the_backlog_and_takes_an_aware_now() -> None:
    receiver(Unheard, key="probe.owed")(lambda event: None)
    _fire()
    EventRecord.objects.update(recorded_at=django_timezone.now() - timedelta(hours=1))
    health = outbox_health(now=datetime.now(timezone.utc))
    assert health.owed == 1
    out = io.StringIO()
    call_command("events_status", "--format", "json", stdout=out)
    age = json.loads(out.getvalue())["oldest_owed_age_seconds"]
    assert 3590 <= age <= 3700


def test_quiet_receivers_compares_with_the_last_success() -> None:
    receiver(Unheard, key="probe.quiet")(lambda event: None)
    _fire()
    drain_outbox()
    assert "probe.quiet" not in {q.key for q in quiet_receivers(within=timedelta(hours=1))}
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    assert "probe.quiet" in {q.key for q in quiet_receivers(within=timedelta(hours=1), now=later)}


def test_prune_reads_its_window_against_the_rows() -> None:
    receiver(Unheard, key="probe.pruned")(lambda event: None)
    _fire()
    drain_outbox()
    prune_events(now=datetime.now(timezone.utc) + timedelta(days=365))
    assert not EventRecord.objects.exists()


def test_a_replay_of_a_scheduled_receiver_is_written_at_its_due_time() -> None:
    receiver(Unheard, key="probe.replayed", due=timedelta(hours=2))(lambda event: None)
    pk = _fire()
    DeliveryRecord.objects.update(status=DeliveryStatus.SUCCEEDED)
    replay_events([pk])
    row = _row("probe.replayed")
    assert row.status == DeliveryStatus.PENDING
    assert row.due_at == row.event.occurred_at + timedelta(hours=2)


def test_a_drain_told_an_aware_time_runs_what_is_due_by_then() -> None:
    ran: list[int] = []
    receiver(Unheard, key="probe.drained", due=timedelta(hours=2))(
        lambda event: ran.append(event.value)
    )
    _fire()
    drain_outbox(now=datetime.now(timezone.utc) + timedelta(hours=3))
    assert ran == [1]


def test_a_relay_on_an_aware_clock_claims_and_delivers() -> None:
    ran: list[int] = []
    receiver(Unheard, key="probe.relayed")(lambda event: ran.append(event.value))
    _fire()
    run_relay(
        worker_id="w",
        passes=1,
        now=lambda: datetime.now(timezone.utc),
        sleep=lambda seconds: None,
        allow_unsafe_concurrency=True,
    )
    assert ran == [1]


def test_handing_back_writes_a_lease_the_backend_accepts() -> None:
    """The handed-back lease is a fixed instant, and it was an aware constant,
    which SQLite refuses to store under this setting."""
    receiver(Unheard, key="probe.handed")(lambda event: None)
    _fire()
    claimed_at = django_timezone.now()
    ids = claim_batch(worker_id="w", now=claimed_at, lease=timedelta(minutes=5), limit=10)
    assert hand_back(ids, worker_id="w", claimed_at=claimed_at) == 1
    assert _row("probe.handed").lease_expires_at < django_timezone.now()


def test_a_task_message_round_trips_its_claim(caplog: pytest.LogCaptureFixture, settings) -> None:
    """The claim a task carries is the ``claimed_at`` read back, as ISO text;
    naive under this setting, and it has to match the column it came from."""
    receiver(Unheard, key="probe.task")(lambda event: None)
    _fire()
    claim_batch(worker_id="relay", now=django_timezone.now(), lease=timedelta(minutes=5), limit=10)
    row = _row("probe.task")
    with caplog.at_level(logging.INFO, logger="django_domain_events.delivery.deliver"):
        outcome = deliver_one(row.pk, claimed_by="relay", claimed_at=row.claimed_at.isoformat())
    assert outcome is DeliveryStatus.SUCCEEDED
    assert "no longer holds" not in caplog.text


def test_a_claim_told_an_aware_time_writes_it_as_local_time() -> None:
    """``claim_batch`` is public, and writes the ``now`` it is told as the claim,
    so it converts the value itself rather than trusting a caller to have."""
    receiver(Unheard, key="probe.claimed")(lambda event: None)
    _fire()
    told = datetime.now(timezone.utc)
    assert len(claim_batch(worker_id="w", now=told, lease=timedelta(minutes=5), limit=10)) == 1
    assert _row("probe.claimed").claimed_at == django_timezone.make_naive(told)


def test_what_a_receiver_and_health_are_handed_is_local_time() -> None:
    seen: list[datetime] = []
    receiver(Unheard, key="probe.context", takes_context=True)(
        lambda event, ctx: seen.append(ctx.due_at)
    )
    _fire()
    assert django_timezone.is_naive(outbox_health().oldest_owed_at)
    drain_outbox()
    assert len(seen) == 1
    assert django_timezone.is_naive(seen[0])
