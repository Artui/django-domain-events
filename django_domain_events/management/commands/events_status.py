from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.outbox_health import outbox_health


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
                        "claimed": health.claimed,
                        "dead": health.dead,
                        "lapsed_leases": health.lapsed_leases,
                        "oldest_owed_age_seconds": age,
                        "receivers": [
                            {
                                "key": r.key,
                                "owed": r.owed,
                                "dead": r.dead,
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
        self.stdout.write(f"claimed       {health.claimed}")
        self.stdout.write(f"dead          {health.dead}")
        self.stdout.write(f"lapsed leases {health.lapsed_leases}")
        self.stdout.write(f"oldest owed   {'-' if age is None else f'{age}s ago'}")
        if not health.receivers:
            self.stdout.write("nothing owed and nothing dead")
            return
        self.stdout.write("")
        for entry in health.receivers:
            oldest = _age(entry.oldest_owed_at)
            self.stdout.write(
                f"  {entry.key}\towed={entry.owed}\tdead={entry.dead}"
                f"\toldest={'-' if oldest is None else f'{oldest}s'}"
            )


def _age(moment: datetime | None) -> int | None:
    """Seconds since a timestamp, which is what a threshold is written against.

    The timestamp itself is useless to an alert rule: it changes every time
    anything is fired, and comparing it needs the reader to know the clock.
    """
    if moment is None:
        return None
    return int((datetime.now(timezone.utc) - moment).total_seconds())
