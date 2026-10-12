"""Tests mirroring ``django_domain_events/models/target_digest_field.py``.

The digest derivation and the column shape are asserted through the model, in
``test_delivery_record.py``. What lives here is the field's contract with the
SQL compiler: which statement a multi-row insert becomes, and that the value
survives every way the package writes, reads and looks it up.
"""

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
    """Django's PostgreSQL compiler gives up the UNNEST form for a whole insert
    when any one field has a placeholder hook, because a hook may answer a
    different placeholder per value. ``BinaryField`` has one, named
    ``get_placeholder`` to 6.0 and ``get_placeholder_sql`` from 6.1, so a field
    inheriting it costs every fan-out a multi-row VALUES statement. Both names,
    because the compiler checks whichever its version calls the hook; this is
    the backend-independent half of the UNNEST test below."""
    field = DeliveryRecord._meta.get_field("target_digest")
    assert not hasattr(field, hook)


def test_the_serialised_field_is_what_the_migrations_already_hold() -> None:
    """Migration 0006 names the class by path and passes no arguments. A change
    to the class that altered either would make ``makemigrations`` write a new
    migration for a column that has not changed."""
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
    """Compiled for a PostgreSQL connection that never connects, so it runs in
    every cell of the matrix rather than only where a server is. The fields are
    the ones ``bulk_create`` hands the compiler for rows without a primary key;
    ``OnConflict.IGNORE`` is replay's ``ignore_conflicts=True``, which appends
    ``ON CONFLICT DO NOTHING`` and keeps the form."""
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
    # One array parameter per column, each holding every row's value: the
    # statement's size no longer grows with the row count.
    assert [len(column) for column in params] == [3] * len(fields)
    assert postgres.connection is None


def test_a_digest_write_on_mysql_carries_no_binary_prefix() -> None:
    """What hiding the hook costs MySQL, pinned so it is a decision rather than
    a drift: its hook answered ``_binary %s``, and an insert or update now
    binds a bare ``%s``, as every lookup on the column always did. Compiled
    through MySQL's operations on a connection that is never opened, since the
    suite runs no MySQL server and the backend itself needs mysqlclient; the
    warning that follows on a server is documented, not tested here."""
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
                # MySQL's operations refuse an aware value under USE_TZ = False.
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
    """The statement fan-out writes, and what replay writes for the targets it
    finds missing. VALUES costs Postgres a parse and plan over one placeholder
    per column per row; UNNEST binds one array per column whatever the count."""
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
    """``save()``, a one-row and a multi-row ``bulk_create``, read back through
    the model and through ``values_list``. Bytes in; bytes, or the read-only
    buffer psycopg 2 returns, out - so compared as bytes."""
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
    """The two lookups replay makes: ``target_digest__in`` to find which
    targets already have a row, and an equality on one digest."""
    event = _event()
    DeliveryRecord.objects.bulk_create(_rows(event, ["a", "b", "c"]))

    assert DeliveryRecord.objects.get(target_digest=_digest("b")).target == "b"
    found = DeliveryRecord.objects.filter(target_digest__in=[_digest("a"), _digest("c")])
    assert sorted(found.values_list("target", flat=True)) == ["a", "c"]
    assert not DeliveryRecord.objects.filter(target_digest=_digest("missing")).exists()
