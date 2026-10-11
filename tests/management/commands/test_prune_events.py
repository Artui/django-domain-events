from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.db import connection, transaction

from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from tests.testapp.events import PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_reports_what_it_deleted(record: list[str]) -> None:
    with transaction.atomic():
        fire(PinnedName(value=1))
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(days=200))

    out = StringIO()
    call_command("prune_events", stdout=out)
    assert "deleted: 1" in out.getvalue()


def test_the_window_can_be_overridden(record: list[str]) -> None:
    with transaction.atomic():
        fire(PinnedName(value=1))
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(days=5))

    out = StringIO()
    call_command("prune_events", "--days", "1", "--limit", "10", stdout=out)
    assert "deleted: 1" in out.getvalue()


def test_the_batch_size_is_counted_in_rows(record: list[str]) -> None:
    """Three events of one delivery each, two rows a batch: each event costs
    its delivery and itself, so three transactions. Read as an event count,
    two would share the first."""
    with transaction.atomic():
        for _ in range(3):
            fire(PinnedName(value=1))
    events = list(EventRecord.objects.all())
    now = datetime.now(timezone.utc)
    DeliveryRecord.objects.bulk_create(
        DeliveryRecord(event=e, receiver_key="testapp.r", available_at=now, status="succeeded")
        for e in events
    )
    EventRecord.objects.update(recorded_at=now - timedelta(days=200))

    deletes: list[str] = []

    def log(execute, sql, params, many, context):  # noqa: ANN001, ANN202
        if sql.startswith("DELETE") and EventRecord._meta.db_table in sql.split("WHERE")[0]:
            deletes.append(sql)
        return execute(sql, params, many, context)

    out = StringIO()
    with connection.execute_wrapper(log):
        call_command("prune_events", "--batch-size", "2", stdout=out)
    assert "deleted: 3" in out.getvalue()
    assert len(deletes) == 3


def test_a_batch_size_below_one_is_refused() -> None:
    with pytest.raises(CommandError, match="batch_size"):
        call_command("prune_events", "--batch-size", "0", stdout=StringIO())
