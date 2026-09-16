"""Tests mirroring ``migrations/0005_deliveryrecord_target.py``.

Driven through Django's migration executor rather than the migration's
functions, because this migration has none: what is worth gating is what a row
written under the old schema looks like under the new one, and only a real
rewind can produce such a row.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import datetime, timezone

import pytest
from django.core.management import call_command
from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor

from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus

pytestmark = pytest.mark.django_db(transaction=True)

APP = "django_domain_events"
BEFORE = [(APP, "0004_backfill_succeeded_at")]
AFTER = [(APP, "0005_deliveryrecord_target")]


@pytest.fixture
def at_the_previous_schema() -> Iterator[MigrationExecutor]:
    """Rewind the app to the migration before the column, and always come back.

    The finally matters more than the rewind: a test that failed here and left
    the schema behind would fail every test that runs after it, and point at
    none of them.
    """
    executor = MigrationExecutor(connection)
    executor.migrate(BEFORE)
    try:
        yield executor
    finally:
        latest = MigrationExecutor(connection)
        latest.migrate(latest.loader.graph.leaf_nodes(APP))


def test_the_models_and_the_migrations_agree() -> None:
    """A constraint edited in the model alone would pass every test built on the
    migrated schema, which is every test."""
    call_command("makemigrations", APP, check=True, dry_run=True, verbosity=0)


def test_a_row_from_before_the_column_reads_blank_and_still_delivers(
    at_the_previous_schema: MigrationExecutor,
) -> None:
    """A new column is empty on upgrade, so what matters is what empty means.

    Blank is what a receiver without ``targets=`` writes, so the old row is
    indistinguishable from a new one and the relay delivers it as it would have.
    """
    old = at_the_previous_schema.loader.project_state(BEFORE).apps
    event = old.get_model(APP, "EventRecord").objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={
            "order_id": 7,
            "total": "19.99",
            "placed_at": "2026-08-31T09:00:00Z",
            "trace": "d29eb6e4-a54c-4f06-8e3b-f1c416264d37",
            "currency": "EUR",
            "kind": "retail",
            "tags": ["gift"],
            "note": None,
        },
        occurred_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )
    old.get_model(APP, "DeliveryRecord").objects.create(
        event_id=event.pk, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )

    MigrationExecutor(connection).migrate(AFTER)

    row = DeliveryRecord.objects.get(receiver_key="testapp.durable_receiver")
    assert row.target == ""
    assert row.target_digest == hashlib.sha256(b"").hexdigest()
    assert deliver_pending() == {DeliveryStatus.SUCCEEDED: 1}

    # And the constraint that replaced the old one still refuses a second blank
    # row for the same pair, which is the old constraint's whole job.
    with pytest.raises(IntegrityError), transaction.atomic():
        DeliveryRecord.objects.create(
            event_id=event.pk,
            receiver_key="testapp.durable_receiver",
            available_at=event.recorded_at,
        )
