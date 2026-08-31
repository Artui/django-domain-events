from __future__ import annotations

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.quiet_receivers import quiet_receivers


class Command(BaseCommand):
    help = "List durable receivers that have succeeded at nothing inside the window."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--days", type=int, default=None, help="Override RETENTION_DAYS.")

    def handle(self, *args: Any, **options: Any) -> None:
        days = options["days"]
        quiet = quiet_receivers(within=None if days is None else timedelta(days=days))
        if not quiet:
            self.stdout.write("Every durable receiver has run inside the window.")
            return
        for entry in quiet:
            last = (
                "never" if entry.last_succeeded_at is None else entry.last_succeeded_at.isoformat()
            )
            self.stdout.write(f"{entry.key}\t{entry.event_name}\t{last}")
