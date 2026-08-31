from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_domain_events.deliver import deliver_pending


class Command(BaseCommand):
    help = "Deliver pending outbox rows once, then exit."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--once", action="store_true", help="Required. One pass, then exit.")
        parser.add_argument("--limit", type=int, default=None, help="Deliver at most this many.")

    def handle(self, *args: Any, **options: Any) -> None:
        # Required rather than defaulted: a continuous relay needs the leased
        # claim that makes two workers safe, so a flag defaulting now would
        # silently mean something different when that lands.
        if not options["once"]:
            raise CommandError(
                "Pass --once. A continuous relay needs the leased claim that "
                "makes two workers safe, which this version does not have."
            )
        counts = deliver_pending(limit=options["limit"])
        if not counts:
            self.stdout.write("Nothing owed.")
            return
        for status, count in sorted(counts.items(), key=lambda pair: pair[0].value):
            self.stdout.write(f"{status.value}: {count}")
