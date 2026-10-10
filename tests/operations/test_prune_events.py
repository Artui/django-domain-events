from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from django.db import connection, models, transaction

from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.models.receiver_last_success import ReceiverLastSuccess
from django_domain_events.operations import prune_events as prune_module
from django_domain_events.operations.prune_events import prune_events
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import Plans, delivery_table_access, event_deleted
from tests.testapp.events import OrderPlaced, PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def _age(days: int) -> None:
    """Backdate every event, since recorded_at is auto_now_add."""
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(days=days))


def test_it_deletes_settled_events_past_the_window(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    _age(120)

    assert prune_events() == 1
    assert not EventRecord.objects.exists()
    assert not DeliveryRecord.objects.exists()


def test_it_never_deletes_an_event_that_is_still_owed(
    order: OrderPlaced, record: list[str]
) -> None:
    """The rule the whole thing turns on. Deleting a row with work outstanding
    drops an obligation nobody recorded as lost, which is the exact failure the
    outbox exists to prevent."""
    with transaction.atomic():
        fire(order)
    _age(365)

    assert prune_events() == 0
    assert EventRecord.objects.exists()


def test_a_delivery_in_flight_protects_its_event(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.CLAIMED
    )
    _age(365)

    assert prune_events() == 0


def test_a_dead_delivery_is_settled(order: OrderPlaced, record: list[str]) -> None:
    """Dead means the package has stopped trying. Holding the event forever
    would make a permanent failure a permanent leak."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD)
    _age(365)

    assert prune_events() == 1


def test_an_event_with_no_deliveries_is_settled(record: list[str]) -> None:
    """A suppressed event, or one with no durable receivers, is settled by
    definition - there is nothing that could still be owed."""
    with transaction.atomic():
        fire(PinnedName(value=1))
    _age(365)

    assert prune_events() == 1


def test_events_inside_the_window_are_kept(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    _age(10)

    assert prune_events(timedelta(days=90)) == 0
    assert prune_events(timedelta(days=1)) == 1


def test_it_deletes_in_batches(order: OrderPlaced, record: list[str]) -> None:
    """A single statement over a year of rows holds a lock for as long as it
    runs, on the table the relay is trying to claim from."""
    with transaction.atomic():
        for _ in range(7):
            fire(PinnedName(value=1))
    _age(365)

    assert prune_events(batch_size=2) == 7


def test_a_limit_stops_it_early(record: list[str]) -> None:
    with transaction.atomic():
        for _ in range(7):
            fire(PinnedName(value=1))
    _age(365)

    assert prune_events(limit=3) == 3
    assert EventRecord.objects.count() == 4


def test_a_replay_between_the_select_and_the_delete_is_respected(
    order: OrderPlaced, record: list[str]
) -> None:
    """Settledness is re-checked at the delete. A replay landing in between makes
    rows owed again, and the cascade would take them with no record that anything
    was lost - after the operator had been told they were reopened."""
    from django_domain_events.operations.replay_events import replay_events

    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()
    _age(365)

    # The interleaving, made deterministic: the event is settled when prune
    # chooses it and owed again by the time prune writes.
    assert replay_events([event_id])["reopened"] == 2
    assert prune_events() == 0
    assert EventRecord.objects.filter(pk=event_id).exists()


def test_it_counts_events_not_cascaded_rows(order: OrderPlaced, record: list[str]) -> None:
    """delete() reports every object it removed, including the delivery rows the
    cascade takes - so a caller asking how many events went would be told how
    many rows went."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    _age(365)

    assert prune_events() == 1


# Per-event retention. These build their rows directly rather than through
# fire(): the prune reads only the two columns fire() copies onto the row, and
# never the registry, which is the property under test - the fate of an event
# is settled by what it was recorded with.

SUCCEEDED = DeliveryStatus.SUCCEEDED
DEAD = DeliveryStatus.DEAD


def _recorded(
    *statuses: DeliveryStatus,
    delete_when: str = "",
    retention_seconds: int | None = None,
    age: timedelta = timedelta(0),
) -> EventRecord:
    """One event with one delivery row per status, recorded ``age`` ago."""
    now = datetime.now(timezone.utc)
    event = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        payload={},
        occurred_at=now,
        delete_when=delete_when,
        retention_seconds=retention_seconds,
    )
    EventRecord.objects.filter(pk=event.pk).update(recorded_at=now - age)
    DeliveryRecord.objects.bulk_create(
        DeliveryRecord(
            event=event,
            receiver_key=f"testapp.r{n}",
            status=status,
            available_at=now,
            succeeded_at=now if status == SUCCEEDED else None,
        )
        for n, status in enumerate(statuses)
    )
    return event


def test_a_fan_out_with_one_dead_row_is_kept_until_every_delivery_succeeded() -> None:
    """``succeeded`` waits for success: the dead letter keeps its event, so it
    stays inspectable and ``requeue_dead`` still has something to reopen."""
    kept = _recorded(SUCCEEDED, SUCCEEDED, DEAD, delete_when="succeeded")

    assert prune_events() == 0
    assert DeliveryRecord.objects.filter(event=kept).count() == 3


def test_the_same_fan_out_is_deleted_once_settled() -> None:
    """``settled`` waits only for every delivery to be terminal, dead letters
    included, so the event goes without waiting out the window."""
    _recorded(SUCCEEDED, SUCCEEDED, DEAD, delete_when="settled")

    assert prune_events() == 1
    assert not EventRecord.objects.exists()
    assert not DeliveryRecord.objects.exists()


def test_every_delivery_succeeded_deletes_it_at_once() -> None:
    _recorded(SUCCEEDED, SUCCEEDED, delete_when="succeeded")

    assert prune_events() == 1
    assert not DeliveryRecord.objects.exists()


def test_an_orphaned_delivery_keeps_an_event_waiting_for_success() -> None:
    """Orphaned is terminal but not a success: nothing ran, so the event has not
    been consumed."""
    _recorded(SUCCEEDED, DeliveryStatus.ORPHANED, delete_when="succeeded")

    assert prune_events() == 0


@pytest.mark.parametrize("delete_when", ["succeeded", "settled"])
@pytest.mark.parametrize(
    "owed", [DeliveryStatus.PENDING, DeliveryStatus.FAILED, DeliveryStatus.CLAIMED]
)
def test_an_owed_delivery_keeps_it_under_either_policy(
    delete_when: str, owed: DeliveryStatus
) -> None:
    _recorded(SUCCEEDED, owed, delete_when=delete_when)

    assert prune_events() == 0


@pytest.mark.parametrize("delete_when", ["succeeded", "settled"])
def test_an_event_with_no_durable_deliveries_is_consumed_as_soon_as_it_commits(
    delete_when: str,
) -> None:
    """Nothing is owed, so nothing is left to wait for. A suppressed event is
    this case too: its row goes at the first sweep."""
    _recorded(delete_when=delete_when)

    assert prune_events() == 1


def test_an_ordinary_event_is_not_deleted_on_consumption() -> None:
    _recorded(SUCCEEDED, SUCCEEDED)

    assert prune_events() == 0


def test_an_event_kept_by_a_dead_letter_still_goes_at_the_ordinary_window() -> None:
    """The dead letter keeps the event inspectable for RETENTION_DAYS, not
    forever: without this the policy that deletes sooner would keep longer."""
    _recorded(SUCCEEDED, DEAD, delete_when="succeeded", age=timedelta(days=120))

    assert prune_events() == 1


def test_an_unknown_policy_falls_back_to_the_ordinary_window() -> None:
    """A value no release writes - a hand edit, or a downgrade - is neither
    consumed early nor kept forever."""
    _recorded(SUCCEEDED, delete_when="bogus")
    _recorded(SUCCEEDED, delete_when="bogus", age=timedelta(days=120))

    assert prune_events() == 1
    assert EventRecord.objects.count() == 1


def test_an_event_with_its_own_window_goes_when_that_window_passes() -> None:
    hour = 3600
    _recorded(SUCCEEDED, retention_seconds=hour, age=timedelta(hours=2))
    kept = _recorded(SUCCEEDED, retention_seconds=hour, age=timedelta(minutes=30))

    assert prune_events() == 1
    assert list(EventRecord.objects.values_list("pk", flat=True)) == [kept.pk]


def test_its_own_window_still_waits_for_every_delivery_to_settle() -> None:
    _recorded(SUCCEEDED, DeliveryStatus.PENDING, retention_seconds=3600, age=timedelta(days=2))

    assert prune_events() == 0


def test_an_event_with_a_longer_window_of_its_own_outlives_retention_days() -> None:
    """The ordinary window is the default, not a ceiling."""
    _recorded(SUCCEEDED, retention_seconds=365 * 86400, age=timedelta(days=120))

    assert prune_events() == 0


def test_older_than_overrides_only_the_default_window() -> None:
    """``--days`` replaces RETENTION_DAYS. An event that declared its own window
    keeps it."""
    _recorded(SUCCEEDED, retention_seconds=30 * 86400, age=timedelta(days=10))

    assert prune_events(timedelta(days=1)) == 0


def test_the_fate_is_read_off_the_row_not_the_registry(
    order: OrderPlaced, record: list[str]
) -> None:
    """An event recorded under the ordinary window keeps it whether or not its
    class is still declared: the prune never asks the registry."""
    with transaction.atomic():
        fire(order)
    drain_outbox()
    with event_deleted("testapp.OrderPlaced"):
        assert prune_events() == 0
        _age(120)
        assert prune_events() == 1


class _DeleteLog:
    """The rows each DELETE statement removed, in order, by table."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, int]] = []

    def __call__(
        self, execute: Callable[..., Any], sql: str, params: Any, many: bool, context: Any
    ) -> Any:
        result = execute(sql, params, many, context)
        if sql.startswith("DELETE"):
            target = sql.split("WHERE")[0]
            table = "event" if EventRecord._meta.db_table in target else "delivery"
            self.statements.append((table, context["cursor"].rowcount))
        return result


def test_a_batch_is_bounded_by_rows_not_events() -> None:
    """Four events of three deliveries each, four rows a batch: one event per
    transaction, because each costs its deliveries plus itself. Counted in
    events, as batches used to be, the first would take all four and twelve
    delivery rows with them."""
    for _ in range(4):
        _recorded(SUCCEEDED, SUCCEEDED, SUCCEEDED, delete_when="succeeded")

    log = _DeleteLog()
    with connection.execute_wrapper(log):
        assert prune_events(batch_size=4) == 4
    assert log.statements == [("delivery", 3), ("event", 1)] * 4


def test_an_event_with_no_deliveries_still_counts_against_the_batch() -> None:
    """The event row is a row. Otherwise a million suppressed events would be
    one transaction."""
    for _ in range(5):
        _recorded(delete_when="settled")

    log = _DeleteLog()
    with connection.execute_wrapper(log):
        assert prune_events(batch_size=2) == 5
    assert [rows for table, rows in log.statements if table == "event"] == [2, 2, 1]


def test_an_event_larger_than_the_batch_is_deleted_in_chunks() -> None:
    """Its delivery rows go a batch at a time, each chunk its own transaction,
    and the event row with the last of them."""
    _recorded(*[SUCCEEDED] * 10, delete_when="succeeded")

    log = _DeleteLog()
    with connection.execute_wrapper(log):
        assert prune_events(batch_size=4) == 1
    assert log.statements == [("delivery", 4), ("delivery", 4), ("delivery", 2), ("event", 1)]
    assert not DeliveryRecord.objects.exists()


@pytest.mark.parametrize("size", [0, -1, True, "2", 2.5])
def test_a_batch_size_that_is_not_a_positive_count_is_refused(size: object) -> None:
    """One case per condition of the guard: zero and a negative by the lower
    bound (zero would delete nothing and report success), ``True`` by the bool
    test (an int, so it would read as one), and a string and a float by the
    type test (a setting read from the environment arrives as the first)."""
    with pytest.raises(ValueError, match="batch_size"):
        prune_events(batch_size=size)


@pytest.mark.parametrize(
    ("delete_when", "retention_seconds", "age"),
    [
        ("succeeded", None, timedelta(0)),
        ("settled", None, timedelta(0)),
        ("", 3600, timedelta(hours=2)),
        ("", None, timedelta(days=120)),
    ],
    ids=["succeeded", "settled", "own-window", "ordinary"],
)
def test_a_stale_selection_is_rechecked_at_the_delete(
    monkeypatch: pytest.MonkeyPatch,
    delete_when: str,
    retention_seconds: int | None,
    age: timedelta,
) -> None:
    """The selection handed to the delete names an event that a replay has
    since made owed again. The delete must re-check, or the cascade takes the
    reopened work with no record that anything was lost. The selection is
    stubbed because a replay that lands before the prune starts is simply not
    selected, and proves nothing about the delete."""
    event = _recorded(
        SUCCEEDED,
        DeliveryStatus.PENDING,
        delete_when=delete_when,
        retention_seconds=retention_seconds,
        age=age,
    )
    stale = iter([[event.pk]])
    monkeypatch.setattr(prune_module, "_candidates", lambda due, take: next(stale, []))

    assert prune_events() == 0
    assert DeliveryRecord.objects.filter(event=event).count() == 2


def test_a_stale_selection_is_rechecked_before_a_chunk_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same for an event too large for one batch, whose delivery rows go
    before it does: a chunk is deleted only while its event is still due."""
    event = _recorded(
        SUCCEEDED, SUCCEEDED, SUCCEEDED, DeliveryStatus.PENDING, delete_when="settled"
    )
    stale = iter([[event.pk]])
    monkeypatch.setattr(prune_module, "_candidates", lambda due, take: next(stale, []))

    assert prune_events(batch_size=2) == 0
    assert DeliveryRecord.objects.filter(event=event).count() == 4


def _successes() -> dict[str, datetime]:
    return dict(ReceiverLastSuccess.objects.values_list("receiver_key", "last_succeeded_at"))


def test_each_receiver_s_last_success_outlives_its_deliveries() -> None:
    """The newest success among what went, per receiver - and nothing for a
    receiver whose deleted rows never succeeded."""
    _recorded(SUCCEEDED, DEAD, delete_when="settled")
    newest = _recorded(SUCCEEDED, delete_when="settled")
    latest = datetime.now(timezone.utc) + timedelta(minutes=5)
    DeliveryRecord.objects.filter(event=newest).update(succeeded_at=latest)

    assert prune_events() == 2
    assert _successes() == {"testapp.r0": latest}


def test_a_prune_never_moves_a_last_success_backwards() -> None:
    later = datetime.now(timezone.utc) + timedelta(days=1)
    ReceiverLastSuccess.objects.create(receiver_key="testapp.r0", last_succeeded_at=later)
    _recorded(SUCCEEDED, delete_when="succeeded")

    assert prune_events() == 1
    assert _successes() == {"testapp.r0": later}


def test_a_prune_moves_a_last_success_forwards() -> None:
    earlier = datetime.now(timezone.utc) - timedelta(days=1)
    ReceiverLastSuccess.objects.create(receiver_key="testapp.r0", last_succeeded_at=earlier)
    _recorded(SUCCEEDED, delete_when="succeeded")

    assert prune_events() == 1
    assert _successes()["testapp.r0"] > earlier


def test_a_chunk_records_its_last_successes_too() -> None:
    event = _recorded(SUCCEEDED, SUCCEEDED, SUCCEEDED, delete_when="succeeded")
    for n, row in enumerate(DeliveryRecord.objects.filter(event=event).order_by("pk")):
        row.receiver_key = "testapp.fan"
        row.target = f"t{n}"
        row.succeeded_at = datetime(2026, 1, 1 + n, tzinfo=timezone.utc)
        row.save()

    assert prune_events(batch_size=2) == 1
    assert _successes() == {"testapp.fan": datetime(2026, 1, 3, tzinfo=timezone.utc)}


def test_the_last_success_is_written_in_the_delete_s_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A delete that fails takes the record of it back with it, so the table
    never claims a success for rows that are still there - and, the other way
    round, rows are never deleted without it being written."""

    def fail(ids: list[int]) -> int:
        raise RuntimeError("the delete failed")

    _recorded(SUCCEEDED, delete_when="succeeded")
    monkeypatch.setattr(prune_module, "_delete_events", fail)

    with pytest.raises(RuntimeError):
        prune_events()
    assert not ReceiverLastSuccess.objects.exists()


def test_postgres_is_given_an_interval_for_the_window() -> None:
    """Compiled here for the SQLite gate's sake, since the branch only runs on
    Postgres; what it computes there is held by the window tests above, which
    the Postgres job runs. Every other backend gets microseconds, the form its
    datetime arithmetic expects."""
    query = EventRecord.objects.all().query
    compiler = query.get_compiler(using="default")
    seconds = prune_module._Seconds(models.F("retention_seconds")).resolve_expression(query)

    postgres, _ = seconds.as_postgresql(compiler, connection)
    default, _ = seconds.as_sql(compiler, connection)
    assert postgres.endswith("* INTERVAL '1 second')")
    assert default.endswith("* 1000000)")


EVENT_TABLE = EventRecord._meta.db_table


@pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="asserts Postgres query plans; SQLite's planner is not the one a sweep runs on",
)
def test_each_kind_of_due_is_one_index_range(plans_without_seqscan: Plans) -> None:
    """With nothing due, a sweep is three selects, none reading the event
    history: the two policies through the partial index that holds only events
    with one, the ordinary window through ``recorded_at``.

    The partial index serves a query only if its WHERE clause implies the
    index's condition, so this is what fails when the consumption filter is
    phrased any other way - ``delete_when__in``, say. The window arm's
    ``retention_seconds IS NOT NULL`` is not held here: Postgres 17 infers it
    from the strict arithmetic beside it, and the test passes without it.

    It pins the event side. The delivery-side assertion holds at this size
    only: on a delivery table of hundreds of thousands of rows the planner
    may hash the consumed check's subqueries over a full read of that table,
    which ``docs/retention.md`` records as measured."""
    now = datetime.now(timezone.utc)
    EventRecord.objects.bulk_create(
        EventRecord(name="testapp.OrderPlaced", payload={}, occurred_at=now) for _ in range(3000)
    )
    for n in range(50):
        _recorded(DeliveryStatus.PENDING, delete_when="succeeded")
        _recorded(SUCCEEDED, retention_seconds=86400 * 30, age=timedelta(hours=n))
    with connection.cursor() as cursor:
        cursor.execute(f"ANALYZE {EVENT_TABLE}")
        cursor.execute(f"ANALYZE {DeliveryRecord._meta.db_table}")

    plans = plans_without_seqscan(prune_events)

    assert len(plans) == 3, plans
    consumed, own_window, ordinary = plans
    assert "dde_own_retention" in consumed, consumed
    assert "dde_own_retention" in own_window, own_window
    assert "recorded_at" in ordinary and "dde_own_retention" not in ordinary, ordinary
    for plan in plans:
        assert f"Seq Scan on {EVENT_TABLE}" not in plan, plan
        assert "Seq Scan" not in delivery_table_access(plan), plan


def test_the_batch_size_defaults_to_a_setting_of_its_own(settings: Any) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"PRUNE_BATCH_ROWS": 2, "BATCH_SIZE": 1000}
    for _ in range(2):
        _recorded(SUCCEEDED, delete_when="succeeded")

    log = _DeleteLog()
    with connection.execute_wrapper(log):
        assert prune_events() == 2
    assert [rows for table, rows in log.statements if table == "event"] == [1, 1]
