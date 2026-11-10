from __future__ import annotations

from dataclasses import dataclass

from django_domain_events import event


@event
@dataclass(frozen=True, slots=True)
class LateArrival:
    value: int
