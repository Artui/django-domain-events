"""Tests mirroring ``migrations/0006_due_at_retention_and_binary_digest.py``.

Driven through Django's migration executor on populated tables, because the
part worth gating is what happens to rows that already exist: the digest
conversion rewrites every delivery row, in both directions. Every read goes
through the historical models of the state being checked, never the current
ones, which describe neither end of a rewind.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import datetime, timezone
from importlib import import_module
from types import SimpleNamespace
from typing import Any

import pytest
from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor
from django.test.utils import CaptureQueriesContext

pytestmark = pytest.mark.django_db(transaction=True)

APP = "django_domain_events"
BEFORE = [(APP, "0005_deliveryrecord_target")]
AFTER = [(APP, "0006_due_at_retention_and_binary_digest")]

# import_module because the module name starts with a digit.
migration = import_module(f"{APP}.migrations.0006_due_at_retention_and_binary_digest")

# The blank target, a fan-out target, one far past any index entry limit, and
# one whose UTF-8 is not its code points, so a conversion that hashed or
# decoded the wrong encoding would be caught.
TARGETS = ["", "endpoint-42", "x" * 3000, "café-☃"]


@pytest.fixture
def at_the_previous_schema() -> Iterator[MigrationExecutor]:
    """Rewind to the schema before this migration, and always come back."""
    executor = MigrationExecutor(connection)
    executor.migrate(BEFORE)
    try:
        yield executor
    finally:
        latest = MigrationExecutor(connection)
        latest.migrate(latest.loader.graph.leaf_nodes(APP))


def _models(state: list[tuple[str, str]]) -> tuple[Any, Any]:
    apps = MigrationExecutor(connection).loader.project_state(state).apps
    return apps.get_model(APP, "EventRecord"), apps.get_model(APP, "DeliveryRecord")


def _populate_the_old_schema() -> int:
    """Rows as 0005 wrote them: the digest as 64 hex characters, supplied by
    hand because the historical field is a plain string with nothing to derive
    it."""
    event_model, delivery_model = _models(BEFORE)
    event = event_model.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    for target in TARGETS:
        delivery_model.objects.create(
            event_id=event.pk,
            receiver_key="probe.fan",
            target=target,
            target_digest=hashlib.sha256(target.encode()).hexdigest(),
            available_at=event.recorded_at,
        )
    return event.pk


def _stored(state: list[tuple[str, str]]) -> dict[str, Any]:
    _, delivery_model = _models(state)
    return dict(delivery_model.objects.values_list("target", "target_digest"))


def test_every_hex_digest_becomes_the_raw_bytes_it_spelled(
    at_the_previous_schema: MigrationExecutor,
) -> None:
    _populate_the_old_schema()

    MigrationExecutor(connection).migrate(AFTER)

    stored = {target: bytes(digest) for target, digest in _stored(AFTER).items()}
    assert stored == {t: hashlib.sha256(t.encode()).digest() for t in TARGETS}


def test_the_new_columns_arrive_empty_and_mean_what_they_did_before(
    at_the_previous_schema: MigrationExecutor,
) -> None:
    """No backfill: NULL ``due_at`` is read as the event's ``recorded_at``, and
    an event with no retention of its own follows RETENTION_DAYS, which is
    what every event recorded before the columns did."""
    event_id = _populate_the_old_schema()

    MigrationExecutor(connection).migrate(AFTER)

    event_model, delivery_model = _models(AFTER)
    event = event_model.objects.get(pk=event_id)
    assert (event.retention_seconds, event.delete_when) == (None, "")
    assert set(delivery_model.objects.values_list("due_at", flat=True)) == {None}


def test_the_unique_constraint_holds_on_the_converted_digests(
    at_the_previous_schema: MigrationExecutor,
) -> None:
    """A row written after the upgrade for a target an old row already holds is
    refused. That needs the converted value and the newly derived one to be the
    same bytes - a conversion that stored the hex text's own bytes would pass
    the round trip and let every pre-upgrade delivery be written twice."""
    event_id = _populate_the_old_schema()

    MigrationExecutor(connection).migrate(AFTER)

    _, delivery_model = _models(AFTER)
    for target in TARGETS:
        with pytest.raises(IntegrityError), transaction.atomic():
            delivery_model.objects.create(
                event_id=event_id,
                receiver_key="probe.fan",
                target=target,
                target_digest=hashlib.sha256(target.encode()).digest(),
                available_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
            )


def test_reversing_restores_the_hex_digests_exactly(
    at_the_previous_schema: MigrationExecutor,
) -> None:
    _populate_the_old_schema()
    before = _stored(BEFORE)

    MigrationExecutor(connection).migrate(AFTER)
    MigrationExecutor(connection).migrate(BEFORE)

    assert _stored(BEFORE) == before
    assert before == {t: hashlib.sha256(t.encode()).hexdigest() for t in TARGETS}


def test_the_conversion_reaches_every_row_across_batches(
    at_the_previous_schema: MigrationExecutor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Batched so a large table is never read into memory at once. A batch of
    one makes every row its own page, so a paging step that skipped or
    repeated a row would leave one unconverted - in both directions."""
    monkeypatch.setattr(migration, "BATCH", 1)
    _populate_the_old_schema()

    MigrationExecutor(connection).migrate(AFTER)
    assert len({bytes(d) for d in _stored(AFTER).values()}) == len(TARGETS)
    assert all(len(bytes(d)) == 32 for d in _stored(AFTER).values())

    MigrationExecutor(connection).migrate(BEFORE)
    assert _stored(BEFORE) == {t: hashlib.sha256(t.encode()).hexdigest() for t in TARGETS}


class _RecordingPostgresEditor:
    """Stands in for a Postgres schema editor: records the SQL it is handed.

    What lets the SQLite suite, which gates coverage, reach the Postgres
    branch. The real statements run in the guarded test below and in every
    round trip above when the suite runs on Postgres.
    """

    connection = SimpleNamespace(vendor="postgresql")

    def __init__(self) -> None:
        self.executed: list[str] = []

    def quote_name(self, name: str) -> str:
        return f'"{name}"'

    def execute(self, sql: str) -> None:
        self.executed.append(sql)


def test_postgres_converts_the_column_in_place_in_both_directions() -> None:
    state = MigrationExecutor(connection).loader.project_state(BEFORE)
    editor = _RecordingPostgresEditor()
    operation = migration.ConvertDigest()
    # What ``migrate --plan`` and ``sqlmigrate`` print for it.
    assert operation.describe() == "Store the delivery target digest as raw bytes"

    operation.database_forwards(APP, editor, state, state)
    operation.database_backwards(APP, editor, state, state)

    table = '"django_domain_events_deliveryrecord"'
    assert editor.executed == [
        f'ALTER TABLE {table} ALTER COLUMN "target_digest" TYPE bytea '
        "USING decode(\"target_digest\", 'hex')",
        f'ALTER TABLE {table} ALTER COLUMN "target_digest" TYPE varchar(64) '
        "USING encode(\"target_digest\", 'hex')",
    ]


@pytest.mark.skipif(
    connection.vendor != "postgresql", reason="asserts the statement Postgres is sent"
)
def test_postgres_rewrites_the_table_once_instead_of_swapping_columns(
    at_the_previous_schema: MigrationExecutor,
) -> None:
    """The swap updates every row and leaves a dead copy of each until a
    ``VACUUM FULL``; the in-place ``ALTER`` is one compact rewrite. So on
    Postgres no row is updated and no temporary column appears, in either
    direction, and the round trip still restores every hex digest."""
    _populate_the_old_schema()
    before = _stored(BEFORE)

    with CaptureQueriesContext(connection) as forwards:
        MigrationExecutor(connection).migrate(AFTER)
    with CaptureQueriesContext(connection) as backwards:
        MigrationExecutor(connection).migrate(BEFORE)

    for captured, using in ((forwards, "decode"), (backwards, "encode")):
        sql = [q["sql"] for q in captured.captured_queries]
        assert any("TYPE bytea" in s or "TYPE varchar(64)" in s for s in sql if using in s), sql
        assert not any(s.startswith("UPDATE") or "target_digest_raw" in s for s in sql), sql
    assert _stored(BEFORE) == before
