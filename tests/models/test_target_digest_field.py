from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import django
import pytest
from django.db import connection
from django.db.backends.mysql.operations import DatabaseOperations as MySQLOperations
from django.db.models.constants import OnConflict
from django.db.models.sql import InsertQuery, UpdateQuery
from django.db.utils import ConnectionHandler
from django.test.utils import CaptureQueriesContext
from django.utils.timezone import now

from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord

pytestmark = pytest.mark.django_db

# Django's PostgreSQL insert compiler writes a multi-row insert as one
# ``INSERT ... SELECT * FROM UNNEST(...)`` from 5.2, and gives that form up for
# a field with a placeholder hook from 5.2.9 and 6.0. Before 5.2 every insert is
# VALUES, and from 5.2.0 to 5.2.8 the hook was not consulted at all, so neither
# can tell this field's answer apart.
_REFUSES_A_HOOKED_FIELD = django.VERSION >= (5, 2, 9)
_UNNEST_REASON = (
    "Django's PostgreSQL compiler writes UNNEST from 5.2 and refuses it to a "
    "field with a placeholder hook from 5.2.9; earlier versions cannot tell"
)
unnest_capable = pytest.mark.skipif(not _REFUSES_A_HOOKED_FIELD, reason=_UNNEST_REASON)
unnest_capable_server = pytest.mark.skipif(
    connection.vendor != "postgresql" or not _REFUSES_A_HOOKED_FIELD,
    reason=f"needs a PostgreSQL server; {_UNNEST_REASON}",
)


def _event() -> EventRecord:
    return EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
    )


def _digest(target: str) -> bytes:
    return hashlib.sha256(target.encode()).digest()


def _rows(event: EventRecord, targets: list[str]) -> list[DeliveryRecord]:
    return [
        DeliveryRecord(
            event=event, receiver_key="probe.fan", target=target, available_at=event.recorded_at
        )
        for target in targets
    ]


@pytest.mark.parametrize("hook", ["get_placeholder", "get_placeholder_sql"])
def test_the_field_has_no_placeholder_hook_so_postgres_can_unnest_a_bulk_insert(
    hook: str,
) -> None:
    """Any placeholder hook on a field drops the whole insert from UNNEST form."""
    field = DeliveryRecord._meta.get_field("target_digest")
    assert not hasattr(field, hook)


def test_the_serialised_field_is_what_the_migrations_already_hold() -> None:
    """Migration 0006 names the class by path and passes no arguments."""
    field = DeliveryRecord._meta.get_field("target_digest")
    assert field.deconstruct() == (
        "target_digest",
        "django_domain_events.models.target_digest_field.TargetDigestField",
        [],
        {},
    )


@unnest_capable
@pytest.mark.parametrize("on_conflict", [None, OnConflict.IGNORE])
def test_a_bulk_insert_compiles_to_one_unnest_statement_for_postgres(
    on_conflict: OnConflict | None,
) -> None:
    """Compiled for a Postgres connection that never connects, so every cell runs it."""
    postgres = ConnectionHandler(
        {"default": {"ENGINE": "django.db.backends.postgresql", "NAME": "never-opened"}}
    )["default"]
    meta = DeliveryRecord._meta
    rows = [
        DeliveryRecord(
            event_id=1,
            receiver_key="probe.fan",
            target=f"endpoint-{n}",
            available_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
        )
        for n in range(3)
    ]
    query = InsertQuery(DeliveryRecord, on_conflict=on_conflict)
    fields = [f for f in meta.concrete_fields if f is not meta.pk]
    query.insert_values(fields, rows)

    [(sql, params)] = query.get_compiler(connection=postgres).as_sql()

    assert "UNNEST" in sql
    assert "VALUES" not in sql
    assert [len(column) for column in params] == [3] * len(fields)
    assert postgres.connection is None


def test_a_digest_write_on_mysql_carries_no_binary_prefix() -> None:
    """Hiding the hook costs MySQL nothing: inserts bind a bare %s, as lookups do."""
    mysql = ConnectionHandler(
        {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
    )["default"]
    mysql.ops = MySQLOperations(mysql)
    meta = DeliveryRecord._meta
    insert = InsertQuery(DeliveryRecord)
    insert.insert_values(
        [f for f in meta.concrete_fields if f is not meta.pk],
        [
            DeliveryRecord(
                event_id=1,
                receiver_key="probe.fan",
                target=target,
                available_at=now(),
            )
            for target in ("a", "b")
        ],
    )
    update = UpdateQuery(DeliveryRecord)
    update.add_update_values({"target_digest": _digest("a")})

    [(insert_sql, _)] = insert.get_compiler(connection=mysql).as_sql()
    update_sql, _ = update.get_compiler(connection=mysql).as_sql()

    assert insert_sql.startswith("INSERT INTO `")
    assert "_binary" not in insert_sql
    assert update_sql.endswith("SET `target_digest` = %s")


@unnest_capable_server
def test_a_bulk_insert_on_postgres_is_one_unnest_statement() -> None:
    """The statement fan-out writes, and what replay writes for the targets it finds missing."""
    event = _event()
    targets = [f"endpoint-{n}" for n in range(20)]

    with CaptureQueriesContext(connection) as queries:
        DeliveryRecord.objects.bulk_create(_rows(event, targets))

    [insert] = [q["sql"] for q in queries.captured_queries if q["sql"].startswith("INSERT")]
    assert "UNNEST" in insert
    assert "VALUES" not in insert
    stored = dict(DeliveryRecord.objects.filter(event=event).values_list("target", "target_digest"))
    assert {target: bytes(digest) for target, digest in stored.items()} == {
        target: _digest(target) for target in targets
    }


def test_rows_written_every_way_read_back_with_their_digests() -> None:
    """save(), bulk_create and reads through the model and values_list."""
    event = _event()
    saved = DeliveryRecord(
        event=event, receiver_key="probe.fan", target="saved", available_at=event.recorded_at
    )
    saved.save()
    DeliveryRecord.objects.bulk_create(_rows(event, ["single"]))
    DeliveryRecord.objects.bulk_create(_rows(event, ["first", "second", "third"]))

    by_model = {row.target: bytes(row.target_digest) for row in DeliveryRecord.objects.all()}
    by_values = {
        target: bytes(digest)
        for target, digest in DeliveryRecord.objects.values_list("target", "target_digest")
    }
    expected = {t: _digest(t) for t in ("saved", "single", "first", "second", "third")}
    assert by_model == expected
    assert by_values == expected


def test_a_row_is_found_by_its_digest_alone_and_in_a_set() -> None:
    """The two lookups replay makes: target_digest__in, and an equality on one digest."""
    event = _event()
    DeliveryRecord.objects.bulk_create(_rows(event, ["a", "b", "c"]))

    assert DeliveryRecord.objects.get(target_digest=_digest("b")).target == "b"
    found = DeliveryRecord.objects.filter(target_digest__in=[_digest("a"), _digest("c")])
    assert sorted(found.values_list("target", flat=True)) == ["a", "c"]
    assert not DeliveryRecord.objects.filter(target_digest=_digest("missing")).exists()
