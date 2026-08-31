from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.requeue_dead import requeue_dead


class Command(BaseCommand):
    help = "Give dead-lettered deliveries their attempt budget back."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--receiver", default=None, help="Limit to this receiver key.")
        parser.add_argument("--limit", type=int, default=None, help="Requeue at most this many.")

    def handle(self, *args: Any, **options: Any) -> None:
        count = requeue_dead(receiver_key=options["receiver"], limit=options["limit"])
        self.stdout.write(f"requeued: {count}")
