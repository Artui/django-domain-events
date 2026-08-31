"""``deliver_events`` - run what the outbox owes."""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_domain_events.deliver import deliver_pending


class Command(BaseCommand):
    """Deliver pending rows once and exit.

    Only ``--once`` exists at this version, and it is required rather than
    defaulted, because the alternative is a flag that silently means something
    different later. A long-running relay needs a leased claim and
    ``SELECT ... FOR UPDATE SKIP LOCKED`` to be safe against a second copy of
    itself; until that exists, a command that looks like a daemon would be one.
    """

    help = "Deliver pending outbox rows once, then exit."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--once",
            action="store_true",
            help="Required. Make one delivery pass and exit.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Deliver at most this many rows.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
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
