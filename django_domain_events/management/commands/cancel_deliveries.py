from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.operations.cancel_deliveries import cancel_deliveries


class Command(BaseCommand):
    help = "Withdraw the owed deliveries of events, so they never run."

    def add_arguments(self, parser: Any) -> None:
        # One or more, as replay_events takes them: a cancel naming no events
        # is refused by the parser rather than read as every event.
        parser.add_argument("event_ids", nargs="+", type=int)
        parser.add_argument("--receiver", default=None, help="Limit to this receiver key.")

    def handle(self, *args: Any, **options: Any) -> None:
        counts = cancel_deliveries(options["event_ids"], receiver_key=options["receiver"])
        self.stdout.write(f"cancelled: {counts['cancelled']}, in_flight: {counts['in_flight']}")
