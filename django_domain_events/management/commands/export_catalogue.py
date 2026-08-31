from __future__ import annotations

from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.catalogue import catalogue
from django_domain_events.render_catalogue import render_catalogue


class Command(BaseCommand):
    help = "Write the event catalogue: every declared event, its payload and its receivers."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
        parser.add_argument("--output", default=None, help="Write here instead of stdout.")

    def handle(self, *args: Any, **options: Any) -> None:
        document = render_catalogue(catalogue(), format=options["format"])
        destination = options["output"]
        if destination is None:
            # No trailing newline of our own: the renderer already ends with
            # one, and `write` adds a second only when it does not.
            self.stdout.write(document, ending="")
            return
        Path(destination).write_text(document, encoding="utf-8")
        self.stdout.write(f"wrote {destination}")
