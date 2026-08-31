from __future__ import annotations

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.prune_events import prune_events


class Command(BaseCommand):
    help = "Delete settled events older than the retention window."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--days", type=int, default=None, help="Override RETENTION_DAYS.")
        parser.add_argument("--limit", type=int, default=None, help="Delete at most this many.")

    def handle(self, *args: Any, **options: Any) -> None:
        window = None if options["days"] is None else timedelta(days=options["days"])
        deleted = prune_events(window, limit=options["limit"])
        self.stdout.write(f"deleted: {deleted}")
