"""Tests mirroring ``management/commands/events_status.py``."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db


def _run(*args: str) -> str:
    out = StringIO()
    call_command("events_status", *args, stdout=out)
    return out.getvalue()


def test_a_drained_outbox_says_so(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    output = _run()
    assert "owed          0" in output
    assert "nothing owed and nothing dead" in output


def test_it_names_the_receivers_that_are_behind(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    output = _run()
    assert "owed          2" in output
    assert "testapp.durable_receiver\towed=1\tdead=0\toldest=" in output


def test_the_oldest_owed_is_reported_as_an_age(order: OrderPlaced, record: list[str]) -> None:
    """The age is what an alert threshold is written against, not the
    timestamp."""
    with transaction.atomic():
        fire(order)
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(hours=2))
    assert "oldest owed   72" in _run()


def test_an_empty_outbox_has_no_age_to_report() -> None:
    assert "oldest owed   -" in _run()


def test_json_is_parseable_and_carries_the_same_numbers(
    order: OrderPlaced, record: list[str]
) -> None:
    """The shape a metrics agent scrapes, so it has to be machine-readable
    without parsing the human output."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.DEAD, attempts=5
    )
    parsed = json.loads(_run("--format", "json"))
    assert parsed["owed"] == 1
    assert parsed["dead"] == 1
    assert parsed["oldest_owed_age_seconds"] is not None
    by_key = {r["key"]: r for r in parsed["receivers"]}
    assert by_key["testapp.with_context"]["dead"] == 1
    assert by_key["testapp.with_context"]["owed"] == 0
    # The per-receiver age reaches the output a metrics agent scrapes, or the
    # field is documented as the alerting number and unreachable.
    assert by_key["testapp.durable_receiver"]["oldest_owed_age_seconds"] is not None
    assert by_key["testapp.with_context"]["oldest_owed_age_seconds"] is None


def test_json_reports_a_null_age_on_an_empty_outbox() -> None:
    assert json.loads(_run("--format", "json"))["oldest_owed_age_seconds"] is None
