from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import transaction

from django_domain_events.fire import fire
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
