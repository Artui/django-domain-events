from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from django_domain_events.declaration.registry import registry
from django_domain_events.models.lane_pause_record import LanePauseRecord
from django_domain_events.utils import in_project_time

OPERATOR = "operator"


class Command(BaseCommand):
    help = "Stop every delivery path claiming a lane, until a time or for seconds."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("lane")
        # The relay's cleanup deletes a record whose until has passed, so an open pause has none.
        bound = parser.add_mutually_exclusive_group(required=True)
        bound.add_argument("--for", dest="seconds", type=float, help="Pause for this many seconds.")
        bound.add_argument("--until", help="Pause until this ISO 8601 time.")

    def handle(self, *args: Any, **options: Any) -> None:
        lane = options["lane"]
        try:
            registry.require_lane(lane)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        now = timezone.now()
        until = _until(now, options["seconds"], options["until"])
        if until <= now:
            raise CommandError(f"--until {until.isoformat()} has already passed.")
        LanePauseRecord.objects.update_or_create(
            lane=lane,
            worker_id=OPERATOR,
            defaults={"paused_at": now, "until": until},
        )
        self.stdout.write(f"paused lane {lane} until {until.isoformat()}")


def _until(now: datetime, seconds: float | None, until: str | None) -> datetime:
    """``now`` plus ``seconds``, or ``until`` (read in ``TIME_ZONE`` if naive), as stored."""
    if seconds is not None:
        if not (math.isfinite(seconds) and seconds > 0):
            raise CommandError(f"--for must be a positive number of seconds, got {seconds:g}.")
        try:
            return now + timedelta(seconds=seconds)
        except OverflowError as exc:
            raise CommandError(f"--for {seconds:g} ends past the last date there is.") from exc
    parsed = parse_datetime(until or "")
    if parsed is None:
        raise CommandError(f"--until {until!r} is not an ISO 8601 date and time.")
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_default_timezone())
    return in_project_time(parsed)
