from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from django.core.management.base import BaseCommand
from django.utils import timezone

from django_domain_events.introspection.outbox_health import outbox_health


class Command(BaseCommand):
    help = "Report how far behind the outbox is."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--format", choices=["text", "json"], default="text")

    def handle(self, *args: Any, **options: Any) -> None:
        health = outbox_health()
        age = _age(health.oldest_owed_at)

        if options["format"] == "json":
            self.stdout.write(
                json.dumps(
                    {
                        "owed": health.owed,
                        "scheduled": health.scheduled,
                        "claimed": health.claimed,
                        "dead": health.dead,
                        "lapsed_leases": health.lapsed_leases,
                        "oldest_owed_age_seconds": age,
                        "lanes": [
                            {
                                "lane": b.lane,
                                "owed": b.owed,
                                "scheduled": b.scheduled,
                                "claimed": b.claimed,
                                "dead": b.dead,
                                "oldest_owed_age_seconds": _age(b.oldest_owed_at),
                                "paused": [
                                    {
                                        "worker_id": p.worker_id,
                                        "paused_for_seconds": _left(p.until),
                                    }
                                    for p in b.pauses
                                ],
                            }
                            for b in health.lanes
                        ],
                        "receivers": [
                            {
                                "key": r.key,
                                "owed": r.owed,
                                "dead": r.dead,
                                "scheduled": r.scheduled,
                                "claimed": r.claimed,
                                "oldest_owed_age_seconds": _age(r.oldest_owed_at),
                            }
                            for r in health.receivers
                        ],
                    },
                    indent=2,
                )
            )
            return

        self.stdout.write(f"owed          {health.owed}")
        self.stdout.write(f"scheduled     {health.scheduled}")
        self.stdout.write(f"claimed       {health.claimed}")
        self.stdout.write(f"dead          {health.dead}")
        self.stdout.write(f"lapsed leases {health.lapsed_leases}")
        self.stdout.write(f"oldest owed   {'-' if age is None else f'{age}s ago'}")
        # The lanes come first because a lane is what an operator scales and
        # pauses; the receivers below say which reaction inside it is behind.
        self.stdout.write("")
        self.stdout.write("lanes")
        for lane in health.lanes:
            oldest = _age(lane.oldest_owed_at)
            self.stdout.write(
                f"  {lane.lane}\towed={lane.owed}\tdead={lane.dead}"
                f"\tscheduled={lane.scheduled}\tclaimed={lane.claimed}"
                f"\toldest={'-' if oldest is None else f'{oldest}s'}"
            )
            for pause in lane.pauses:
                self.stdout.write(f"    paused by {pause.worker_id} for {_left(pause.until)}s more")
        self.stdout.write("")
        if not health.receivers:
            self.stdout.write("nothing owed and nothing dead")
            return
        self.stdout.write("receivers")
        for entry in health.receivers:
            oldest = _age(entry.oldest_owed_at)
            self.stdout.write(
                f"  {entry.key}\towed={entry.owed}\tdead={entry.dead}"
                f"\tscheduled={entry.scheduled}\tclaimed={entry.claimed}"
                f"\toldest={'-' if oldest is None else f'{oldest}s'}"
            )


def _age(moment: datetime | None) -> int | None:
    """Seconds since a timestamp, which is what a threshold is written against.

    The timestamp itself is useless to an alert rule: it changes every time
    anything is fired, and comparing it needs the reader to know the clock.
    """
    if moment is None:
        return None
    return int((timezone.now() - moment).total_seconds())


def _left(moment: datetime) -> int:
    """Seconds until a timestamp: how long a pause has to run.

    The time left rather than the end, for the reason ``_age`` gives an age:
    a threshold or an operator deciding whether to wait reads a duration.
    """
    return int((moment - timezone.now()).total_seconds())
