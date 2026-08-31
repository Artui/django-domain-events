"""The test helper for asserting an event was recorded."""

from __future__ import annotations

from typing import TypeVar

from django_domain_events.registry import registry
from django_domain_events.settings import get_codec

E = TypeVar("E")


def assert_fired(event_class: type[E], *, times: int | None = None) -> list[E]:
    """Assert an event was fired, and return the decoded events.

    Reads the log rather than patching ``fire``: a mock records that a function
    was called, while the row is what the rest of the system reacts to. Decoding
    on the way out means a payload that cannot round-trip fails here too.
    """
    from django_domain_events.models.event_record import EventRecord

    entry = registry.event_for_class(event_class)
    if entry is None:
        raise LookupError(
            f"{event_class.__name__} is not registered, so it cannot have been "
            f"fired. Decorate it with @event."
        )

    rows = list(EventRecord.objects.filter(name=entry.name).order_by("pk"))
    if times is None:
        assert rows, f"Expected {entry.name} to have been fired, but it was not."
    else:
        assert len(rows) == times, (
            f"Expected {entry.name} to have been fired {times} time(s), found {len(rows)}."
        )

    codec = get_codec()
    return [codec.decode(event_class, row.payload, row.version) for row in rows]
