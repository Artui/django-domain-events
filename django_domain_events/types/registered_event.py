from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from django_domain_events.types.retention import Retention


@dataclass(frozen=True, slots=True)
class RegisteredEvent:
    event_class: type
    name: str
    version: int

    retention: timedelta | Retention | None = None
    """How long ``fire()`` records this event for: its own window, a
    :class:`Retention` policy, or None for ``RETENTION_DAYS``.

    Last, and defaulted, so adding it does not break a consumer constructing
    one - this is an exported type."""

    @property
    def retention_columns(self) -> tuple[int | None, str]:
        """``retention`` as the event row records it: ``(retention_seconds,
        delete_when)``.

        Exactly one of the two is set, or neither. The schema cannot say so -
        the check-constraint keyword changed inside the supported Django range
        - so this is where it holds, for ``fire()`` and the catalogue alike.
        """
        if isinstance(self.retention, timedelta):
            return int(self.retention.total_seconds()), ""
        if isinstance(self.retention, Retention):
            return None, self.retention.value
        return None, ""
