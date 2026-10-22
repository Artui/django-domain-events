"""Tests mirroring ``django_domain_events/outbox_health.py``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone as django_timezone

from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.introspection.outbox_health import outbox_health
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.models.lane_pause_record import LanePauseRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.outbox_health import OutboxHealth
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import TERMINAL
from tests.conftest import (
    Plans,
    delivery_table_access,
    event_registered,
    receiver_deleted,
    receiver_registered,
)
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
    stuck = django_timezone.now() - timedelta(hours=3)
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
        available_at=django_timezone.now() + timedelta(minutes=30),
    )

    oldest = outbox_health().oldest_owed_at
    assert oldest == recorded
    assert oldest <= django_timezone.now(), "the age must never be negative"


def test_a_settled_row_does_not_hold_the_oldest_timestamp_open(
    order: OrderPlaced, record: list[str]
) -> None:
    """Draining the queue must clear it, or the alert never resets."""
    with transaction.atomic():
        fire(order)
    EventRecord.objects.update(recorded_at=django_timezone.now() - timedelta(days=2))
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
    now = django_timezone.now()
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1", lease_expires_at=now - timedelta(minutes=1)
    )
    assert outbox_health(now=now).lapsed_leases == 2


def test_a_live_lease_is_not_counted_as_lapsed(order: OrderPlaced, record: list[str]) -> None:
    now = django_timezone.now()
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


def test_the_per_receiver_split_says_who_holds_the_claims(
    order: OrderPlaced, record: list[str]
) -> None:
    """The total says rows are claimed; only the split says whose. A receiver
    whose rows are all claimed and none finishing is a wedged receiver or a
    queue holding its tasks, where the same total spread evenly is a relay at
    work. The counts add up to the total, so the split loses nothing."""
    with transaction.atomic():
        fire(order)
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1"
    )
    health = outbox_health()
    by_key = {r.key: r for r in health.receivers}
    assert by_key["testapp.with_context"].claimed == 2
    assert by_key["testapp.durable_receiver"].claimed == 0
    assert sum(r.claimed for r in health.receivers) == health.claimed == 2


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
    stuck = django_timezone.now() - timedelta(hours=5)
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
    now = django_timezone.now()
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
    now = django_timezone.now()
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
    due = django_timezone.now() + timedelta(hours=1)
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
    _scheduled(django_timezone.now() + timedelta(days=1), status=DeliveryStatus.DEAD)

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
        available_at=django_timezone.now() + timedelta(days=1),
        due_at=django_timezone.now() + timedelta(days=1),
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
    # The pause table is read fourth and last, which is what keeps the three
    # above describing the delivery table's queries.
    assert len(plans) == 4, plans
    assert LanePauseRecord._meta.db_table in plans[3], plans


def test_a_receiver_with_only_scheduled_work_sorts_after_one_that_is_behind(
    order: OrderPlaced, record: list[str]
) -> None:
    """Whatever the keys: here the scheduled receiver's key sorts first, so
    only ranking by what is owed puts it last."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        available_at=django_timezone.now() + timedelta(days=1),
        due_at=django_timezone.now() + timedelta(days=1),
    )

    assert [(e.key, e.owed, e.scheduled) for e in outbox_health().receivers] == [
        ("testapp.with_context", 1, 0),
        ("testapp.durable_receiver", 0, 1),
    ]


def test_a_consumer_constructing_outbox_health_positionally_keeps_working() -> None:
    """``lanes`` is defaulted and comes after ``scheduled``, so a call written
    before it existed still lands each value in the field it meant."""
    health = OutboxHealth(0, 0, 0, None, 0, (), 5)
    assert health.scheduled == 5
    assert health.lanes == ()


# Per-lane health. The test app's receivers all sit in the default lane, so
# these declare two lanes of their own, each with one receiver on an event
# nothing else listens to. The names are chosen against the alphabet: the lane
# that sorts last, ``zulu``, is the one made worst, so an ordering that fell
# back to the registry's sorted lane names would read differently.


@dataclass(frozen=True, slots=True)
class Mike:
    value: int


@dataclass(frozen=True, slots=True)
class Zulu:
    value: int


def _lane_receiver(lane: str, event_class: type) -> RegisteredReceiver:
    return RegisteredReceiver(
        key=f"tests.{lane}",
        event_class=event_class,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="health",
        lane=lane,
    )


@pytest.fixture
def lanes() -> Iterator[None]:
    with (
        event_registered(Mike, "tests.Mike"),
        event_registered(Zulu, "tests.Zulu"),
        receiver_registered(_lane_receiver("mike", Mike)),
        receiver_registered(_lane_receiver("zulu", Zulu)),
    ):
        yield


def _fire(*events: Any) -> None:
    with transaction.atomic():
        for event in events:
            fire(event)


def _counts(health: OutboxHealth) -> list[tuple[Any, ...]]:
    """Each lane as (lane, owed, scheduled, claimed, dead), in the order given."""
    return [(b.lane, b.owed, b.scheduled, b.claimed, b.dead) for b in health.lanes]


def _pause(lane: str, worker_id: str, until: datetime) -> None:
    LanePauseRecord.objects.create(
        lane=lane, worker_id=worker_id, paused_at=until - timedelta(hours=3), until=until
    )


def test_an_empty_outbox_still_lists_the_default_lane() -> None:
    """A lane is listed whether or not anything is owed in it, so a dashboard
    has a series per lane rather than one that vanishes when it drains."""
    (default,) = outbox_health().lanes
    assert (default.lane, default.owed, default.scheduled, default.claimed, default.dead) == (
        "default",
        0,
        0,
        0,
        0,
    )
    assert (default.oldest_owed_at, default.pauses) == (None, ())


def test_every_declared_lane_is_listed_with_zeros(lanes: None) -> None:
    assert _counts(outbox_health()) == [
        ("default", 0, 0, 0, 0),
        ("mike", 0, 0, 0, 0),
        ("zulu", 0, 0, 0, 0),
    ]


def test_a_lane_sums_its_receivers_and_carries_the_oldest_of_them(
    order: OrderPlaced, record: list[str], lanes: None
) -> None:
    """Both test-app receivers are in the default lane. The older owed row is
    made the one of the receiver that ranks *second*, so a rollup taking the
    first receiver's age rather than the minimum reads the wrong one."""
    _fire(order, replace(order, order_id=8))
    now = django_timezone.now()
    stuck = now - timedelta(hours=5)
    later = now + timedelta(days=1)
    durable = DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").order_by("pk")
    context = DeliveryRecord.objects.filter(receiver_key="testapp.with_context").order_by("pk")
    DeliveryRecord.objects.filter(pk=durable[0].pk).update(due_at=stuck)
    DeliveryRecord.objects.filter(pk=durable[1].pk).update(due_at=later, available_at=later)
    DeliveryRecord.objects.filter(pk=context[0].pk).update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1"
    )
    DeliveryRecord.objects.filter(pk=context[1].pk).update(status=DeliveryStatus.DEAD)

    health = outbox_health(now=now)

    assert [r.key for r in health.receivers] == [
        "testapp.with_context",
        "testapp.durable_receiver",
    ]
    assert _counts(health)[0] == ("default", 2, 1, 1, 1)
    assert health.lanes[0].oldest_owed_at == stuck
    # Nothing is lost or counted twice in the rollup.
    assert sum(b.owed for b in health.lanes) == health.owed
    assert sum(b.scheduled for b in health.lanes) == health.scheduled
    assert sum(b.claimed for b in health.lanes) == health.claimed
    assert sum(b.dead for b in health.lanes) == health.dead


def test_a_named_lane_counts_its_own_receivers_and_no_others(
    order: OrderPlaced, record: list[str], lanes: None
) -> None:
    _fire(order, Zulu(value=1), Zulu(value=2), Mike(value=1))
    stuck = django_timezone.now() - timedelta(hours=2)
    DeliveryRecord.objects.filter(receiver_key="tests.zulu").update(due_at=stuck)

    health = outbox_health()

    by_lane = {b.lane: b for b in health.lanes}
    assert (by_lane["zulu"].owed, by_lane["zulu"].oldest_owed_at) == (2, stuck)
    assert by_lane["mike"].owed == 1
    assert by_lane["default"].owed == 2


def test_a_deleted_receivers_rows_are_counted_in_the_default_lane(
    order: OrderPlaced, record: list[str], lanes: None
) -> None:
    """A key no receiver holds drains through the default lane, so that is
    where its backlog shows - not in the lane it was declared in, and not
    nowhere. With the receiver gone ``zulu`` is declared by nobody, so it is
    not listed either."""
    _fire(Zulu(value=1), Zulu(value=2))
    with receiver_deleted("tests.zulu"):
        health = outbox_health()
    assert _counts(health) == [("default", 2, 0, 0, 0), ("mike", 0, 0, 0, 0)]


def test_lanes_are_ranked_by_owed_then_dead_then_name(lanes: None) -> None:
    """The receivers' ranking. ``zulu`` is owed the most and sorts last by
    name; ``mike`` owes nothing but has a dead row, so it comes before the
    default lane, which sorts before it by name; and ``alpha``, a paused lane
    nobody declares, ties the default lane on both counts and is put first by
    its name alone."""
    now = django_timezone.now()
    _fire(Zulu(value=1), Zulu(value=2), Mike(value=1))
    DeliveryRecord.objects.filter(receiver_key="tests.mike").update(status=DeliveryStatus.DEAD)
    _pause("alpha", "host:1", now + timedelta(hours=1))

    assert _counts(outbox_health(now=now)) == [
        ("zulu", 2, 0, 0, 0),
        ("mike", 0, 0, 0, 1),
        ("alpha", 0, 0, 0, 0),
        ("default", 0, 0, 0, 0),
    ]


def test_a_lanes_unexpired_pauses_are_shown_and_expired_ones_are_not(lanes: None) -> None:
    """A pause ends at its ``until`` exactly, as the relay reads it, so a row
    whose ``until`` is now is over. Two relays pausing one lane are two
    pauses, ordered by worker, which here is not the order of their ends."""
    now = django_timezone.now()
    _pause("zulu", "host:2", now + timedelta(hours=1))
    _pause("zulu", "host:1", now + timedelta(hours=2))
    _pause("mike", "host:3", now)

    by_lane = {b.lane: b for b in outbox_health(now=now).lanes}

    assert [(p.worker_id, p.until) for p in by_lane["zulu"].pauses] == [
        ("host:1", now + timedelta(hours=2)),
        ("host:2", now + timedelta(hours=1)),
    ]
    assert by_lane["zulu"].pauses[0].paused_at == now - timedelta(hours=1)
    assert by_lane["mike"].pauses == ()
    assert by_lane["default"].pauses == ()


def test_a_paused_lane_nobody_declares_is_listed_with_zero_counts() -> None:
    """A lane renamed while a relay of the old name still runs: its pause is
    real and an operator needs to see it, though no receiver is in it any
    more. Once the pause has passed, the lane is gone."""
    now = django_timezone.now()
    _pause("renamed", "host:1", now + timedelta(minutes=5))
    _pause("retired", "host:2", now - timedelta(seconds=1))

    health = outbox_health(now=now)

    assert _counts(health) == [("default", 0, 0, 0, 0), ("renamed", 0, 0, 0, 0)]
    assert health.lanes[1].oldest_owed_at is None
    assert [p.worker_id for p in health.lanes[1].pauses] == ["host:1"]


def test_the_pause_table_is_read_last_and_the_rollup_costs_no_query() -> None:
    """Four queries: the three over the delivery table, and the pause table
    after them. The lane counts come from the per-receiver split already
    read, never a query of their own; and the order is what keeps
    ``test_no_query_outbox_health_sends_scans_the_delivery_table``, which
    reads the first three plans, about the delivery table."""
    pause_table = LanePauseRecord._meta.db_table
    with CaptureQueriesContext(connection) as captured:
        outbox_health()
    statements = [query["sql"] for query in captured.captured_queries]
    assert len(statements) == 4, statements
    assert pause_table in statements[3]
    assert not any(pause_table in sql for sql in statements[:3])
