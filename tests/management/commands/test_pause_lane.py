from __future__ import annotations

from datetime import datetime, timedelta
from datetime import timezone as dt_timezone
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.utils import timezone

from django_domain_events.models.lane_pause_record import LanePauseRecord

pytestmark = pytest.mark.django_db


def _run(*args: str) -> str:
    out = StringIO()
    call_command("pause_lane", *args, stdout=out)
    return out.getvalue()


def _record() -> tuple[str, str, datetime, datetime]:
    (row,) = LanePauseRecord.objects.values_list("lane", "worker_id", "paused_at", "until")
    return row


def test_for_pauses_the_lane_as_the_operator_for_that_many_seconds() -> None:
    before = timezone.now()
    output = _run("default", "--for", "90")
    after = timezone.now()

    lane, worker_id, paused_at, until = _record()
    assert (lane, worker_id) == ("default", "operator")
    assert before <= paused_at <= after
    assert until - paused_at == timedelta(seconds=90)
    assert output.startswith("paused lane default until ")


def _aware(value: datetime) -> datetime:
    """A stored value as an aware one, read in ``TIME_ZONE`` when ``USE_TZ`` is off."""
    return timezone.make_aware(value) if timezone.is_naive(value) else value


def _ahead(hours: int) -> datetime:
    return datetime.now(dt_timezone.utc).replace(microsecond=0) + timedelta(hours=hours)


def test_until_reads_a_naive_time_in_the_projects_time_zone() -> None:
    at = timezone.localtime(_ahead(24))
    _run("default", "--until", at.replace(tzinfo=None).isoformat())

    assert _aware(_record()[3]) == at


def test_until_takes_an_offset_as_written() -> None:
    at = _ahead(2).astimezone(timezone.get_fixed_timezone(330))
    _run("default", "--until", at.isoformat())

    assert _aware(_record()[3]) == at


def test_pausing_again_moves_the_operators_record() -> None:
    _run("default", "--for", "60")
    _run("default", "--for", "3600")

    _, _, paused_at, until = _record()
    assert until - paused_at == timedelta(hours=1)


def test_a_relays_record_of_the_lane_is_left_alone() -> None:
    now = timezone.now()
    LanePauseRecord.objects.create(
        lane="default", worker_id="host:1", paused_at=now, until=now + timedelta(hours=1)
    )
    _run("default", "--for", "60")

    assert sorted(LanePauseRecord.objects.values_list("worker_id", flat=True)) == [
        "host:1",
        "operator",
    ]


def test_a_pause_with_no_end_is_refused() -> None:
    """The relays delete a record whose until has passed, so an open-ended one is not offered."""
    with pytest.raises(CommandError, match="one of the arguments --for --until is required"):
        _run("default")
    assert not LanePauseRecord.objects.exists()


def test_for_and_until_together_are_refused() -> None:
    with pytest.raises(CommandError, match="not allowed with argument"):
        _run("default", "--for", "60", "--until", "2099-01-01T00:00:00")


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--for", "0"], "--for must be a positive number of seconds, got 0"),
        (["--for", "-5"], "--for must be a positive number of seconds, got -5"),
        (["--for", "nan"], "--for must be a positive number of seconds, got nan"),
        (["--for", "inf"], "--for must be a positive number of seconds, got inf"),
        (["--for", "1e12"], "--for 1e[+]12 ends past the last date there is"),
        (["--until", "tomorrow"], "'tomorrow' is not an ISO 8601 date and time"),
        (["--until", "2001-01-01T00:00:00+00:00"], "has already passed"),
    ],
)
def test_an_end_that_is_not_ahead_is_refused(args: list[str], message: str) -> None:
    with pytest.raises(CommandError, match=message):
        _run("default", *args)
    assert not LanePauseRecord.objects.exists()


def test_a_lane_no_receiver_declares_is_refused() -> None:
    """A typo would pause nothing, and say it had."""
    with pytest.raises(CommandError, match="No receiver is declared in lane 'mial'"):
        _run("mial", "--for", "60")
    assert not LanePauseRecord.objects.exists()
