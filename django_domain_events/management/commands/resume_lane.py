from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand

from django_domain_events.models.lane_pause_record import LanePauseRecord


class Command(BaseCommand):
    help = "Lift every pause of a lane, an operator's and the relays' alike."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("lane")

    def handle(self, *args: Any, **options: Any) -> None:
        lane = options["lane"]
        deleted, _ = LanePauseRecord.objects.filter(lane=lane).delete()
        self.stdout.write(f"resumed lane {lane}: lifted {deleted} pauses")
