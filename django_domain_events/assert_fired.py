"""The test helper for asserting an event was recorded."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django_domain_events.registry import registry

if TYPE_CHECKING:
    # Annotation-only. ``from __future__ import annotations`` keeps these out
    # of the runtime import graph, which is what lets the model imports stay
    # inside the functions that query them.
    from django_domain_events.models.event_record import EventRecord


def assert_fired(event_class: type, *, times: int | None = None) -> list[EventRecord]:
    """Assert an event was fired, and return the rows.

    Reads the log rather than patching ``fire``, so it asserts what a consumer
    would actually find: a mock records that a function was called, while the row
    is the thing the rest of the system reacts to. If the payload could not be
    encoded, a mock still passes.

    ``times=None`` asserts at least one.
    """
    # See fire(): a module-level model import would run during app loading.
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
    return rows
