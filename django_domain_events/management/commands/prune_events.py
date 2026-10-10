from __future__ import annotations

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_domain_events.operations.prune_events import prune_events


class Command(BaseCommand):
    help = (
        "Delete the events that are due: consumed under a Retention policy, past "
        "their own window, or past the retention window, and settled."
    )

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--days",
            type=int,
            default=None,
            help="Override RETENTION_DAYS. An event with a window of its own keeps it.",
        )
        parser.add_argument("--limit", type=int, default=None, help="Delete at most this many.")
        parser.add_argument(
            "--batch-size",
            type=int,
            default=None,
            help="Rows per transaction, delivery rows included. Overrides PRUNE_BATCH_ROWS.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        window = None if options["days"] is None else timedelta(days=options["days"])
        try:
            deleted = prune_events(window, limit=options["limit"], batch_size=options["batch_size"])
        except ValueError as exc:
            # The operation's own refusal, as a command error: an exit status
            # and one line, rather than a traceback in a cron mail.
            raise CommandError(str(exc)) from exc
        self.stdout.write(f"deleted: {deleted}")
