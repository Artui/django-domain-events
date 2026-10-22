"""Tests mirroring ``management/commands/events_status.py``."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import transaction
from django.utils import timezone as django_timezone

from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.models.lane_pause_record import LanePauseRecord
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
    assert "testapp.durable_receiver\towed=1\tdead=0\tscheduled=0\tclaimed=0\toldest=" in output


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


def _schedule_one(key: str) -> None:
    due = datetime.now(timezone.utc) + timedelta(days=14)
    DeliveryRecord.objects.filter(receiver_key=key).update(available_at=due, due_at=due)


def test_scheduled_work_is_reported_apart_from_what_is_owed(
    order: OrderPlaced, record: list[str]
) -> None:
    """The command is what CI and an operator read, so the split ``outbox_health``
    makes has to reach it: a row due in a fortnight is neither owed nor aging."""
    with transaction.atomic():
        fire(order)
    _schedule_one("testapp.durable_receiver")
    output = _run()
    assert "owed          1" in output
    assert "scheduled     1" in output
    assert "testapp.durable_receiver\towed=0\tdead=0\tscheduled=1\tclaimed=0\toldest=-" in output


def test_json_carries_the_scheduled_counts(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    _schedule_one("testapp.durable_receiver")
    parsed = json.loads(_run("--format", "json"))
    assert (parsed["owed"], parsed["scheduled"]) == (1, 1)
    by_key = {r["key"]: r for r in parsed["receivers"]}
    assert by_key["testapp.durable_receiver"]["scheduled"] == 1
    assert by_key["testapp.with_context"]["scheduled"] == 0


def test_each_receiver_says_how_many_of_its_rows_are_claimed(
    order: OrderPlaced, record: list[str]
) -> None:
    """The per-receiver claimed count reaches both outputs, so the operator and
    the scraper can each tell a wedged receiver from a relay at work."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1"
    )
    output = _run()
    assert "testapp.with_context\towed=1\tdead=0\tscheduled=0\tclaimed=1\toldest=" in output
    parsed = json.loads(_run("--format", "json"))
    by_key = {r["key"]: r for r in parsed["receivers"]}
    assert by_key["testapp.with_context"]["claimed"] == 1
    assert by_key["testapp.durable_receiver"]["claimed"] == 0


def test_cancelled_work_is_neither_owed_nor_dead(order: OrderPlaced, record: list[str]) -> None:
    """Withdrawn work is done, so an outbox holding only cancelled rows reads
    as drained, in the text an operator reads and the JSON a scraper does, and
    no receiver is listed as behind."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.CANCELLED)

    output = _run()
    assert "owed          0" in output
    assert "nothing owed and nothing dead" in output
    parsed = json.loads(_run("--format", "json"))
    assert (parsed["owed"], parsed["dead"], parsed["scheduled"]) == (0, 0, 0)
    assert parsed["receivers"] == []


def _pause(lane: str, worker_id: str, left: timedelta) -> None:
    now = django_timezone.now()
    LanePauseRecord.objects.create(lane=lane, worker_id=worker_id, paused_at=now, until=now + left)


def test_json_carries_each_lane_and_how_long_its_pauses_have_left(
    order: OrderPlaced, record: list[str]
) -> None:
    """A lane renamed while a relay of the old name still runs is listed with
    its pause and nothing owed. ``paused_for_seconds`` is the time left, which
    is what an operator deciding whether to wait it out needs."""
    with transaction.atomic():
        fire(order)
    EventRecord.objects.update(recorded_at=datetime.now(timezone.utc) - timedelta(hours=2))
    DeliveryRecord.objects.filter(receiver_key="testapp.with_context").update(
        status=DeliveryStatus.CLAIMED, claimed_by="w1"
    )
    _pause("renamed", "host:1", timedelta(hours=1))
    _pause("renamed", "host:2", timedelta(minutes=10))

    lanes = json.loads(_run("--format", "json"))["lanes"]

    assert [lane["lane"] for lane in lanes] == ["default", "renamed"]
    default, renamed = lanes
    assert {k: default[k] for k in ("owed", "scheduled", "claimed", "dead", "paused")} == {
        "owed": 2,
        "scheduled": 0,
        "claimed": 1,
        "dead": 0,
        "paused": [],
    }
    assert 7200 <= default["oldest_owed_age_seconds"] < 7260
    assert (renamed["owed"], renamed["oldest_owed_age_seconds"]) == (0, None)
    assert [p["worker_id"] for p in renamed["paused"]] == ["host:1", "host:2"]
    first, second = (p["paused_for_seconds"] for p in renamed["paused"])
    assert 3540 < first <= 3600
    assert 540 < second <= 600


def test_json_lists_the_default_lane_on_an_empty_outbox() -> None:
    assert json.loads(_run("--format", "json"))["lanes"] == [
        {
            "lane": "default",
            "owed": 0,
            "scheduled": 0,
            "claimed": 0,
            "dead": 0,
            "oldest_owed_age_seconds": None,
            "paused": [],
        }
    ]


def test_the_text_lists_each_lane_and_its_pauses_above_the_receivers(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)
    _pause("default", "host:1", timedelta(hours=1))

    output = _run()

    lane_line = "  default\towed=2\tdead=0\tscheduled=0\tclaimed=0\toldest="
    assert "\nlanes\n" + lane_line in output
    assert "\n    paused by host:1 for 3" in output
    assert output.index(lane_line) < output.index("\nreceivers\n")
    assert output.index("\nreceivers\n") < output.index("testapp.durable_receiver\towed=")


def test_the_text_lists_the_lanes_of_a_drained_outbox_too(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    output = _run()
    assert "\nlanes\n  default\towed=0\tdead=0\tscheduled=0\tclaimed=0\toldest=-\n" in output
    assert output.index("\nlanes\n") < output.index("nothing owed and nothing dead")
