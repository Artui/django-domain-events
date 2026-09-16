"""Declared only once ``tests.lateapp`` is installed.

Nothing else in the suite imports this module. The substrate's autodiscovery
imports it when the app is installed, which is the moment the event comes into
existence - after whatever the test declared before installing the app.
"""

from __future__ import annotations

from dataclasses import dataclass

from django_domain_events import event


@event
@dataclass(frozen=True, slots=True)
class LateArrival:
    value: int
