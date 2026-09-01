"""Tests mirroring ``django_domain_events/quiet_receivers.py``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.quiet_receivers import quiet_receivers
from django_domain_events.replay_events import replay_events
from django_domain_events.requeue_dead import requeue_dead
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered
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
