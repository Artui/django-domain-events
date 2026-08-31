from __future__ import annotations

import os
import socket
from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.deliver import deliver_pending
from django_domain_events.run_relay import run_relay


class Command(BaseCommand):
    help = "Deliver outbox rows: one pass with --once, or run as a relay."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--once", action="store_true", help="One pass, then exit.")
        parser.add_argument("--limit", type=int, default=None, help="Deliver at most this many.")
        parser.add_argument("--passes", type=int, default=None, help="Stop after this many passes.")
        parser.add_argument("--worker-id", default=None, help="Defaults to host:pid.")

    def handle(self, *args: Any, **options: Any) -> None:
        worker_id = options["worker_id"] or f"{socket.gethostname()}:{os.getpid()}"
        if options["once"]:
            counts = deliver_pending(limit=options["limit"], worker_id=worker_id)
        else:
            counts = run_relay(worker_id=worker_id, passes=options["passes"])
        if not counts:
            self.stdout.write("Nothing owed.")
            return
        for status, count in sorted(counts.items(), key=lambda pair: pair[0].value):
            self.stdout.write(f"{status.value}: {count}")
