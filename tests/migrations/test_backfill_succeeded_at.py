"""Tests mirroring ``migrations/0004_backfill_succeeded_at.py``.

Driven through the migration's own function against the real models rather
than through a rewound schema: what is worth gating is the predicate it selects
by, and a test that rebuilds the historical model would exercise Django's
migration executor instead.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from importlib import import_module
from types import SimpleNamespace

import pytest
from django.apps import apps
from django.db import connection, transaction

from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.quiet_receivers import quiet_receivers
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db

# import_module because the module name starts with a digit, so it is not a
# valid identifier and cannot be reached with an import statement.
backfill = import_module("django_domain_events.migrations.0004_backfill_succeeded_at").backfill


def _run() -> None:
    """Call the migration function the way RunPython does.

    Django hands it a schema editor, and this one reads a single attribute off
    it: ``connection.alias``, so a router sending the log elsewhere is updated
    there rather than on ``default``. Building a real schema editor needs a
    connection with foreign-key checks off, which SQLite refuses inside the
    transaction each test runs in - so these use a stand-in, and
    ``test_the_real_schema_editor_drives_it_too`` runs the genuine object so
    the stand-in cannot describe something Django does not pass.
    """
    backfill(apps, SimpleNamespace(connection=connection))


def _as_upgraded_from_0_4(order: OrderPlaced) -> datetime:
    """The state the upgrade actually produces: completions on record, and the
    new column empty because AddField does not backfill."""
    acknowledged = datetime.now(timezone.utc) - timedelta(days=1)
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.SUCCEEDED, completed_at=acknowledged, succeeded_at=None
    )
    return acknowledged


def test_an_upgrade_keeps_the_success_history(order: OrderPlaced, record: list[str]) -> None:
    """Without this the headline query is wrong on day one for anyone with
    history: every receiver reads as having never succeeded."""
    acknowledged = _as_upgraded_from_0_4(order)
    assert "testapp.durable_receiver" in [q.key for q in quiet_receivers()]

    _run()

    assert "testapp.durable_receiver" not in [q.key for q in quiet_receivers()]
    assert set(DeliveryRecord.objects.values_list("succeeded_at", flat=True)) == {acknowledged}


def test_it_claims_nothing_for_a_delivery_that_never_succeeded(
    order: OrderPlaced, record: list[str]
) -> None:
    """completed_at is when a row settled, not when it worked. A dead letter
    settled too, and must not be backfilled into evidence of success."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.DEAD, attempts=5, completed_at=datetime.now(timezone.utc)
    )

    _run()

    assert DeliveryRecord.objects.filter(succeeded_at__isnull=False).count() == 0


def test_a_replayed_row_has_no_evidence_left_to_recover(
    order: OrderPlaced, record: list[str]
) -> None:
    """The replay cleared completed_at, so nothing survives to backfill from,
    and inventing a timestamp would be worse than the null."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.PENDING, completed_at=None)

    _run()

    assert DeliveryRecord.objects.filter(succeeded_at__isnull=False).count() == 0


def test_it_does_not_overwrite_a_success_recorded_since_the_upgrade(
    order: OrderPlaced, record: list[str]
) -> None:
    """Running it twice, or on a database already past the upgrade, must not
    move a timestamp the delivery path wrote."""
    real = datetime.now(timezone.utc)
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.SUCCEEDED,
        completed_at=real - timedelta(days=30),
        succeeded_at=real,
    )

    _run()

    assert set(DeliveryRecord.objects.values_list("succeeded_at", flat=True)) == {real}


@pytest.mark.django_db(transaction=True)
def test_the_real_schema_editor_drives_it_too(order: OrderPlaced, record: list[str]) -> None:
    """The stand-in above reads one attribute off a schema editor. This one
    passes the object Django actually passes, so the stand-in cannot quietly
    describe an interface the producer does not have."""
    acknowledged = _as_upgraded_from_0_4(order)
    with connection.schema_editor() as editor:
        backfill(apps, editor)
    assert set(DeliveryRecord.objects.values_list("succeeded_at", flat=True)) == {acknowledged}
