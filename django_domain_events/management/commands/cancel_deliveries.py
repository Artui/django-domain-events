from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_domain_events.operations.cancel_deliveries import cancel_deliveries


class Command(BaseCommand):
    help = "Withdraw the owed deliveries of events, so they never run."

    def add_arguments(self, parser: Any) -> None:
        # One or more, as replay_events takes them: a cancel naming no events
        # is refused by the parser rather than read as every event.
        parser.add_argument("event_ids", nargs="+", type=int)
        # Collected as a list only so a second one can be refused. The
        # operation takes one key, and replay_events repeats this flag and uses
        # every value, so a plain store would keep the last and silently drop
        # the rest - leaving the dropped receivers' work owed.
        parser.add_argument(
            "--receiver",
            action="append",
            dest="receiver_keys",
            help="Limit to this receiver key. One receiver per call.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        keys = options["receiver_keys"] or []
        if len(keys) > 1:
            raise CommandError(
                "cancel_deliveries takes one --receiver per call; "
                f"run it once for each of {', '.join(keys)}."
            )
        counts = cancel_deliveries(options["event_ids"], receiver_key=keys[0] if keys else None)
        self.stdout.write(f"cancelled: {counts['cancelled']}, in_flight: {counts['in_flight']}")
