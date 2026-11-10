from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_domain_events.operations.cancel_deliveries import cancel_deliveries


class Command(BaseCommand):
    help = "Withdraw the owed deliveries of events, so they never run."

    def add_arguments(self, parser: Any) -> None:
        # A list only so a repeated flag is refused, not silently reduced to the last value.
        parser.add_argument("event_ids", nargs="+", type=int)
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
