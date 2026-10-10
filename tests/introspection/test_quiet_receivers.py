"""Tests mirroring ``django_domain_events/quiet_receivers.py``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest
from django.db import connection, transaction

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.introspection.quiet_receivers import quiet_receivers
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.replay_events import replay_events
from django_domain_events.operations.requeue_dead import requeue_dead
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import Plans, delivery_table_access, receiver_registered
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db


def _keys() -> list[str]:
    return [q.key for q in quiet_receivers()]


def test_a_receiver_that_has_never_run_is_reported_as_never() -> None:
    """The answer worth having, and the one a query over delivery rows alone
    cannot produce: there is no row to find."""
    quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["testapp.durable_receiver"].last_succeeded_at is None
    assert quiet["testapp.durable_receiver"].event_name == "testapp.OrderPlaced"


def test_only_durable_receivers_are_considered() -> None:
    """An INLINE receiver leaves no row, so it has no history to be quiet
    about. Listing it as silent forever teaches the reader to skip the
    output."""
    keys = _keys()
    assert "testapp.inline_receiver" not in keys
    assert "testapp.on_commit_receiver" not in keys
    assert "testapp.durable_receiver" in keys


def test_a_recent_success_drops_a_receiver_out(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    assert "testapp.durable_receiver" not in _keys()


def test_a_success_older_than_the_window_puts_it_back(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    long_ago = datetime.now(timezone.utc) - timedelta(days=400)
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        succeeded_at=long_ago
    )
    quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["testapp.durable_receiver"].last_succeeded_at == long_ago


def test_the_window_is_the_caller_s_when_they_pass_one(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    assert "testapp.durable_receiver" in [
        q.key for q in quiet_receivers(within=timedelta(seconds=0))
    ]


def test_the_default_window_is_the_retention_setting(
    order: OrderPlaced, record: list[str], settings
) -> None:
    """Not a coincidence of numbers: past retention the prune has deleted the
    evidence, so this is the longest answer the query can honestly give."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        succeeded_at=datetime.now(timezone.utc) - timedelta(days=10)
    )
    settings.DJANGO_DOMAIN_EVENTS = {"RETENTION_DAYS": 30}
    assert "testapp.durable_receiver" not in _keys()
    settings.DJANGO_DOMAIN_EVENTS = {"RETENTION_DAYS": 5}
    assert "testapp.durable_receiver" in _keys()


def test_a_failed_delivery_does_not_count_as_having_run(
    order: OrderPlaced, record: list[str]
) -> None:
    """The question is whether the receiver did its work, not whether the relay
    tried. A row stuck failing for a month is the case this must catch."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        status=DeliveryStatus.FAILED, attempts=3, completed_at=datetime.now(timezone.utc)
    )
    assert "testapp.durable_receiver" in _keys()


def test_now_is_injectable(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    later = datetime.now(timezone.utc) + timedelta(days=400)
    assert "testapp.durable_receiver" in [q.key for q in quiet_receivers(now=later)]


def test_results_are_sorted_by_key() -> None:
    keys = _keys()
    assert keys == sorted(keys)


@dataclass(frozen=True)
class NeverDeclared:
    value: int


def test_a_receiver_for_an_undeclared_event_falls_back_to_the_class_name() -> None:
    """Registering a receiver for a class with no @event is a check error, not
    an import error, so this runs in a project that has one."""
    entry = RegisteredReceiver(
        key="tests.quiet_dangling",
        event_class=NeverDeclared,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
    )
    with receiver_registered(entry):
        quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["tests.quiet_dangling"].event_name == "NeverDeclared"


def test_a_replay_does_not_erase_that_the_receiver_ran(
    order: OrderPlaced, record: list[str]
) -> None:
    """The two features contradicted each other: replay reopens a row and
    clears its completion, so an operator who replayed yesterday's events to
    re-run a receiver they had just fixed was then told it had never run."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    assert "testapp.durable_receiver" not in _keys()

    replay_events(EventRecord.objects.values_list("pk", flat=True))

    quiet = {q.key: q for q in quiet_receivers()}
    assert "testapp.durable_receiver" not in quiet


def test_a_requeue_does_not_erase_it_either(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5)
    requeue_dead()

    assert "testapp.durable_receiver" not in _keys()


def test_a_row_that_never_succeeded_has_no_timestamp_to_keep(
    order: OrderPlaced, record: list[str]
) -> None:
    """The column records success, not settlement: a dead-lettered delivery
    must not read as one that ran."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5)
    quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["testapp.durable_receiver"].last_succeeded_at is None


def test_a_quiet_wildcard_is_reported_under_the_name_it_was_declared_with() -> None:
    """A wildcard is declared for no event, so the name reported is the marker's,
    which is what the declaration in the source says."""
    wildcard = RegisteredReceiver(
        key="testapp.everything",
        event_class=AnyEvent,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
    )
    with receiver_registered(wildcard):
        quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["testapp.everything"].event_name == "AnyEvent"
    assert quiet["testapp.everything"].last_succeeded_at is None


def test_the_latest_success_wins_over_older_ones_and_failures() -> None:
    """Several rows per receiver, so the answer depends on picking the newest
    success rather than any one row: a lookup that read the first row it found,
    or the oldest, passes every one-row test above."""
    event = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    now = datetime.now(timezone.utc)
    newest = now - timedelta(days=1)
    for n, succeeded in enumerate(
        (now - timedelta(days=200), newest, None, now - timedelta(days=50))
    ):
        DeliveryRecord.objects.create(
            event=event,
            receiver_key="testapp.durable_receiver",
            target=f"t{n}",
            available_at=event.recorded_at,
            succeeded_at=succeeded,
        )
    assert "testapp.durable_receiver" not in _keys()
    quiet = {q.key: q for q in quiet_receivers(within=timedelta(hours=1))}
    assert quiet["testapp.durable_receiver"].last_succeeded_at == newest


@pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="asserts Postgres query plans; SQLite's planner is not the one a scrape runs on",
)
def test_each_receiver_costs_one_probe_of_the_last_success_index(
    plans_without_seqscan: Plans,
) -> None:
    """``MAX(succeeded_at)`` for one key is a single descent of the index on
    ``(receiver_key, succeeded_at)``: the planner rewrites it into a backward
    scan under ``LIMIT 1``. Grouped across keys it is not, and reads every row
    the receivers ever had.

    Populated and analyzed first, because the rewrite is chosen on cost: on an
    empty table the planner expects one row per key and an ordinary aggregate
    over the index costs the same, so the plan would not say which query was
    sent."""
    event = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    now = datetime.now(timezone.utc)
    DeliveryRecord.objects.bulk_create(
        DeliveryRecord(
            event=event,
            receiver_key="testapp.durable_receiver",
            target=f"t{n}",
            available_at=now,
            succeeded_at=now - timedelta(seconds=n),
        )
        for n in range(5000)
    )
    with connection.cursor() as cursor:
        cursor.execute(f"ANALYZE {DeliveryRecord._meta.db_table}")
    plans = plans_without_seqscan(quiet_receivers)
    durable = [r for r in registry.receivers() if r.mode is DeliveryMode.DURABLE]
    assert len(plans) == len(durable)
    for plan in plans:
        assert delivery_table_access(plan) == {"dde_last_success"}, plan
    # The receivers with no rows are estimated at one entry, where an ordinary
    # aggregate over it costs the same; the one with history is where the
    # rewrite has to show.
    [busy] = [p for p in plans if "'testapp.durable_receiver'" in p]
    assert "Limit" in busy, busy
    assert "Index Only Scan Backward using dde_last_success" in busy, busy
