from __future__ import annotations

from typing import TypeVar

from django_domain_events.declaration.registry import registry
from django_domain_events.utils import decode_payload

E = TypeVar("E")


def assert_fired(event_class: type[E], *, times: int | None = None) -> list[E]:
    """Assert an event was fired, and return the decoded events."""
    from django_domain_events.models.event_record import EventRecord

    entry = registry.event_for_class(event_class)
    if entry is None:
        raise LookupError(
            f"{event_class.__name__} is not registered, so it cannot have been "
            f"fired. Decorate it with @event."
        )

    rows = list(EventRecord.objects.filter(name=entry.name).order_by("pk"))
    if times is None and not rows:
        raise AssertionError(f"Expected {entry.name} to have been fired, but it was not.")
    if times is not None and len(rows) != times:
        raise AssertionError(
            f"Expected {entry.name} to have been fired {times} time(s), found {len(rows)}."
        )

    return [decode_payload(event_class, row.payload, row.version) for row in rows]
