from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.replay_events import replay_events


class Command(BaseCommand):
    help = "Make events owed again, for the receivers registered right now."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("event_ids", nargs="+", type=int)
        parser.add_argument(
            "--receiver",
            action="append",
            dest="receiver_keys",
            help="Limit to this receiver key. Repeatable.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        counts = replay_events(options["event_ids"], receiver_keys=options["receiver_keys"])
        self.stdout.write(f"reopened: {counts['reopened']}, added: {counts['added']}")
