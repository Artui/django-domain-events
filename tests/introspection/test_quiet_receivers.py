from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest
from django.db import connection, transaction
from django.utils import timezone as django_timezone

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.introspection.quiet_receivers import quiet_receivers
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.models.receiver_last_success import ReceiverLastSuccess
from django_domain_events.operations.prune_events import prune_events
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
    """The answer a delivery-row query cannot give: there is no row to find."""
    quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["testapp.durable_receiver"].last_succeeded_at is None
    assert quiet["testapp.durable_receiver"].event_name == "testapp.OrderPlaced"


def _pruned_after_running(order: OrderPlaced, *, skip: str | None = None) -> None:
    """Fire, deliver, and prune the event and its delivery rows away."""
    with transaction.atomic():
        fire(order)
    if skip is not None:
        DeliveryRecord.objects.filter(receiver_key=skip).delete()
    drain_outbox()
    EventRecord.objects.update(recorded_at=django_timezone.now() - timedelta(days=365))
    assert prune_events() == 1
    assert not DeliveryRecord.objects.exists()


def test_a_receiver_whose_deliveries_were_pruned_still_reports_its_last_success(
    order: OrderPlaced, record: list[str]
) -> None:
    """A sweep deletes consumed events and their rows, so run history can vanish."""
    _pruned_after_running(order)

    assert "testapp.durable_receiver" not in _keys()
    assert "testapp.with_context" not in _keys()


def test_a_receiver_that_never_ran_is_still_quiet_after_a_prune(
    order: OrderPlaced, record: list[str]
) -> None:
    _pruned_after_running(order, skip="testapp.durable_receiver")

    quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["testapp.durable_receiver"].last_succeeded_at is None
    assert "testapp.with_context" not in quiet


@pytest.mark.parametrize("pruned_is_later", [True, False])
def test_the_later_of_the_pruned_record_and_the_live_rows_is_reported(
    pruned_is_later: bool,
) -> None:
    now = django_timezone.now()
    live, pruned = now - timedelta(days=3), now - timedelta(days=2)
    if not pruned_is_later:
        live, pruned = pruned, live
    event = EventRecord.objects.create(name="testapp.OrderPlaced", payload={}, occurred_at=now)
    DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=now, succeeded_at=live
    )
    ReceiverLastSuccess.objects.create(
        receiver_key="testapp.durable_receiver", last_succeeded_at=pruned
    )

    quiet = {q.key: q for q in quiet_receivers(within=timedelta(days=1))}
    assert quiet["testapp.durable_receiver"].last_succeeded_at == max(live, pruned)


def test_only_durable_receivers_are_considered() -> None:
    """An INLINE receiver leaves no row, so it has no history to be quiet about."""
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
    long_ago = django_timezone.now() - timedelta(days=400)
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
    """Past retention the prune has deleted the evidence, so this is the honest answer."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        succeeded_at=django_timezone.now() - timedelta(days=10)
    )
    settings.DJANGO_DOMAIN_EVENTS = {"RETENTION_DAYS": 30}
    assert "testapp.durable_receiver" not in _keys()
    settings.DJANGO_DOMAIN_EVENTS = {"RETENTION_DAYS": 5}
    assert "testapp.durable_receiver" in _keys()


def test_a_failed_delivery_does_not_count_as_having_run(
    order: OrderPlaced, record: list[str]
) -> None:
    """The question is whether the receiver did its work, not whether the relay tried."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        status=DeliveryStatus.FAILED, attempts=3, completed_at=django_timezone.now()
    )
    assert "testapp.durable_receiver" in _keys()


def test_now_is_injectable(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    later = django_timezone.now() + timedelta(days=400)
    assert "testapp.durable_receiver" in [q.key for q in quiet_receivers(now=later)]


def test_results_are_sorted_by_key() -> None:
    keys = _keys()
    assert keys == sorted(keys)


@dataclass(frozen=True)
class NeverDeclared:
    value: int


def test_a_receiver_for_an_undeclared_event_falls_back_to_the_class_name() -> None:
    """A receiver for a class with no @event is a check error, not an import error."""
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
    """Replay clears completion, so a re-run read as never run."""
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
    """The column records success, not settlement: a dead letter must not read as run."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5)
    quiet = {q.key: q for q in quiet_receivers()}
    assert quiet["testapp.durable_receiver"].last_succeeded_at is None


def test_a_quiet_wildcard_is_reported_under_the_name_it_was_declared_with() -> None:
    """A wildcard is declared for no event, so the name reported is the marker's."""
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
    """Several rows per receiver: the answer must pick the newest success, not any row."""
    event = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    now = django_timezone.now()
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
    """MAX(succeeded_at) per key is one index descent: a backward scan under LIMIT 1."""
    event = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    now = django_timezone.now()
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
    [pruned] = [p for p in plans if ReceiverLastSuccess._meta.db_table in p]
    assert DeliveryRecord._meta.db_table not in pruned, pruned
    plans.remove(pruned)
    assert len(plans) == len(durable)
    for plan in plans:
        assert delivery_table_access(plan) == {"dde_last_success"}, plan
    [busy] = [p for p in plans if "'testapp.durable_receiver'" in p]
    assert "Limit" in busy, busy
    assert "Index Only Scan Backward using dde_last_success" in busy, busy
