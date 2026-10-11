"""Tests mirroring ``django_domain_events/outbox_health.py``."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from django.db import connection, transaction

from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.introspection.outbox_health import outbox_health
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.utils import TERMINAL
from tests.conftest import Plans, delivery_table_access
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db


def test_an_empty_outbox_is_all_zeroes_and_no_timestamp() -> None:
    health = outbox_health()
    assert (health.owed, health.claimed, health.dead, health.lapsed_leases) == (0, 0, 0, 0)
    assert health.oldest_owed_at is None
    assert health.receivers == ()


def test_owed_counts_what_the_relay_will_pick_up(order: OrderPlaced, record: list[str]) -> None:
    """Owed means not terminal, the same definition the relay claims by, so
    this cannot say the queue is empty while the relay still has work."""
    with transaction.atomic():
        fire(order)
    assert outbox_health().owed == 2

    drain_outbox()
    assert outbox_health().owed == 0


def test_a_claimed_row_is_still_owed(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.CLAIMED, claimed_by="w1")
    health = outbox_health()
    assert (health.owed, health.claimed) == (2, 2)


def test_the_oldest_owed_timestamp_is_when_the_work_arrived(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)
    stuck = datetime.now(timezone.utc) - timedelta(hours=3)
    EventRecord.objects.update(recorded_at=stuck)
    assert outbox_health().oldest_owed_at == stuck


def test_a_failing_receiver_does_not_produce_a_negative_age(
    order: OrderPlaced, record: list[str]
) -> None:
    """The backoff pushes available_at into the future on every failed
    attempt, so an age read off that column goes negative exactly while a
    receiver is failing - which is when the alert has to fire."""
    with transaction.atomic():
        fire(order)
    recorded = EventRecord.objects.get().recorded_at
    DeliveryRecord.objects.update(
        status=DeliveryStatus.FAILED,
        attempts=2,
        available_at=datetime.now(timezone.utc) + timedelta(minutes=30),
    )

    oldest = outbox_health().oldest_owed_at
    assert oldest == recorded
    assert oldest <= datetime.now(timezone.utc), "the age must never be negative"


def test_a_settled_row_does_not_hold_the_oldest_timestamp_open(
    order: OrderPlaced, record: list[str]
) -> None:
    """Draining the queue must clear it, or the alert never resets."""
    with transaction.atomic():
        fire(order)
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(days=2))
    drain_outbox()
    assert outbox_health().oldest_owed_at is None


def test_dead_is_counted_but_not_owed(order: OrderPlaced, record: list[str]) -> None:
    """A dead letter is not work the relay will do. Counting it as owed would
    make a backlog alert fire forever after one bad deploy."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5)
    health = outbox_health()
    assert (health.owed, health.dead) == (0, 2)


def test_a_lapsed_lease_is_visible(order: OrderPlaced, record: list[str]) -> None:
    """Steady non-zero means workers are dying mid-delivery, or a receiver
    outruns its lease and has its work thrown away every time."""
    now = datetime.now(timezone.utc)
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1", lease_expires_at=now - timedelta(minutes=1)
    )
    assert outbox_health(now=now).lapsed_leases == 2


def test_a_live_lease_is_not_counted_as_lapsed(order: OrderPlaced, record: list[str]) -> None:
    now = datetime.now(timezone.utc)
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1", lease_expires_at=now + timedelta(hours=1)
    )
    assert outbox_health(now=now).lapsed_leases == 0


def test_the_per_receiver_split_names_who_is_behind(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.DEAD, attempts=5
    )
    by_key = {r.key: r for r in outbox_health().receivers}
    assert by_key["testapp.durable_receiver"].owed == 1
    assert by_key["testapp.durable_receiver"].dead == 0
    assert by_key["testapp.with_context"].owed == 0
    assert by_key["testapp.with_context"].dead == 1


def test_receivers_with_nothing_owed_or_dead_are_left_out(
    order: OrderPlaced, record: list[str]
) -> None:
    """A list naming every declared receiver every time is a list nobody
    reads."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    assert outbox_health().receivers == ()


def test_the_worst_backlog_comes_first(order: OrderPlaced, record: list[str]) -> None:
    """An operator scanning the output reads the top line first, so it has to
    be the receiver furthest behind."""
    with transaction.atomic():
        fire(order)
        fire(replace(order, order_id=8))
    # The receiver registered *second* is made the worst one, so the expected
    # order differs from the order the rows come back in. Starving the first
    # instead passes with no sort at all.
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").order_by(
        "pk"
    ).first().delete()

    entries = outbox_health().receivers
    assert [(e.key, e.owed) for e in entries] == [
        ("testapp.with_context", 2),
        ("testapp.durable_receiver", 1),
    ]


def test_a_receiver_backlog_carries_its_own_oldest(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
        fire(replace(order, order_id=9))
    stuck = datetime.now(timezone.utc) - timedelta(hours=5)
    oldest_event = EventRecord.objects.order_by("pk").first()
    EventRecord.objects.filter(pk=oldest_event.pk).update(recorded_at=stuck)
    # The other receiver is settled, so only one backlog is left to carry it.
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.SUCCEEDED
    )
    by_key = {r.key: r for r in outbox_health().receivers}
    assert by_key["testapp.durable_receiver"].oldest_owed_at == stuck


def _scheduled(due: datetime, **fields: object) -> None:
    """Make every row one scheduled for ``due``, as ``fire()`` writes it."""
    DeliveryRecord.objects.update(due_at=due, **{"available_at": due, **fields})


def test_a_scheduled_row_is_not_owed_and_does_not_age_before_its_due_time(
    order: OrderPlaced, record: list[str]
) -> None:
    """A row due in fourteen days is work that has not arrived. Counting it
    owed and aging it from the event would trip a five-minute backlog alert
    five minutes after the event and hold it for the fortnight."""
    with transaction.atomic():
        fire(order)
    now = datetime.now(timezone.utc)
    EventRecord.objects.update(recorded_at=now - timedelta(hours=1))
    _scheduled(now + timedelta(days=14))

    health = outbox_health(now=now)

    assert (health.owed, health.oldest_owed_at) == (0, None)
    assert health.scheduled == 2
    by_key = {r.key: r for r in health.receivers}
    assert (
        by_key["testapp.durable_receiver"].owed,
        by_key["testapp.durable_receiver"].scheduled,
    ) == (
        0,
        1,
    )
    assert by_key["testapp.durable_receiver"].oldest_owed_at is None


def test_a_scheduled_row_failing_after_its_due_time_ages_from_its_due_time(
    order: OrderPlaced, record: list[str]
) -> None:
    """Work arrives when it becomes due, so that is what its age counts from -
    not the event three days earlier, which would read as a three-day backlog
    the moment the row came due, and not ``available_at``, which the backoff
    pushes ahead into a negative age."""
    with transaction.atomic():
        fire(order)
    now = datetime.now(timezone.utc)
    due = now - timedelta(minutes=10)
    EventRecord.objects.update(recorded_at=now - timedelta(days=3))
    _scheduled(due, status=DeliveryStatus.FAILED, attempts=1)
    DeliveryRecord.objects.update(available_at=now + timedelta(minutes=30))

    health = outbox_health(now=now)

    assert health.oldest_owed_at == due
    assert {r.oldest_owed_at for r in health.receivers} == {due}
    assert (health.owed, health.scheduled) == (2, 0)


def test_a_row_becomes_owed_at_exactly_its_due_time(order: OrderPlaced, record: list[str]) -> None:
    """The claim takes a row once ``available_at <= now``, so health has to call
    it owed at the same instant, or a row the relay could already take would be
    reported as not having arrived."""
    with transaction.atomic():
        fire(order)
    due = datetime.now(timezone.utc) + timedelta(hours=1)
    _scheduled(due)

    before = outbox_health(now=due - timedelta(microseconds=1))
    at = outbox_health(now=due)
    assert (before.owed, at.owed) == (0, 2)
    assert (before.scheduled, at.scheduled) == (2, 0)


def test_a_settled_row_is_never_scheduled_whatever_its_due_time(
    order: OrderPlaced, record: list[str]
) -> None:
    """A settled row keeps the due time it was written with, and a row settled
    before that time came - dead-lettered by hand, or any terminal status a
    later release adds - is not work waiting for it. The per-receiver split
    reads owed and dead rows in one query, so its scheduled count has to be
    narrowed to the owed ones as well as to the not yet due."""
    with transaction.atomic():
        fire(order)
    _scheduled(datetime.now(timezone.utc) + timedelta(days=1), status=DeliveryStatus.DEAD)

    health = outbox_health()

    assert (health.dead, health.scheduled) == (2, 0)
    assert {(r.dead, r.scheduled) for r in health.receivers} == {(1, 0)}


def test_a_receiver_with_only_scheduled_work_is_listed(
    order: OrderPlaced, record: list[str]
) -> None:
    """Listed after every receiver that is behind, but listed: the per-receiver
    split is also where an operator sees which reactions are waiting."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        available_at=datetime.now(timezone.utc) + timedelta(days=1),
        due_at=datetime.now(timezone.utc) + timedelta(days=1),
    )

    entries = outbox_health().receivers
    assert [(e.key, e.owed) for e in entries] == [
        ("testapp.durable_receiver", 1),
        ("testapp.with_context", 0),
    ]
    assert [(e.key, e.owed, e.scheduled) for e in entries] == [
        ("testapp.durable_receiver", 1, 0),
        ("testapp.with_context", 0, 1),
    ]


def test_every_status_is_counted_owed_or_settled_exactly_once() -> None:
    """Owed is phrased as the partial indexes' own predicates, which lists the
    owed statuses where ``exclude(status__in=TERMINAL)`` could not miss one.
    So this is what goes red if a status is added and the phrasing is not: one
    row in every status there is, and the owed count must be every status that
    is not terminal."""
    event = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    for status in DeliveryStatus:
        DeliveryRecord.objects.create(
            event=event,
            receiver_key=f"probe.{status.value}",
            status=status,
            available_at=event.recorded_at,
        )
    health = outbox_health()
    owed = {s.value for s in DeliveryStatus if s not in TERMINAL}
    assert health.owed == len(owed) == 3
    assert health.dead == 1
    assert {b.key for b in health.receivers if b.owed} == {f"probe.{s}" for s in owed}
    assert {b.key for b in health.receivers if b.dead} == {"probe.dead"}


postgres_only = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="asserts Postgres query plans; SQLite's planner is not the one a scrape runs on",
)


@postgres_only
def test_no_query_outbox_health_sends_scans_the_delivery_table(
    plans_without_seqscan: Plans,
) -> None:
    """Each predicate is the partial indexes' own, so each query reads only
    the owed and dead rows however much delivered history the table holds.
    A ``status NOT IN (terminal)`` predicate would match none of the owed or
    dead indexes and read the whole table on every scrape."""
    owed = {"dde_owed_by_available_at", "dde_claimed_by_lease"}
    dead = {"dde_dead_by_receiver"}
    plans = plans_without_seqscan(outbox_health)
    access = [delivery_table_access(plan) for plan in plans]
    assert access[:2] == [owed, dead], plans
    # Owed-or-dead implies "not succeeded", so the prune's index of unfinished
    # rows serves the per-receiver split too, in one scan where the three
    # above take a bitmap OR; it holds no succeeded row either.
    assert access[2] in (owed | dead, {"dde_unfinished_by_event"}), plans
