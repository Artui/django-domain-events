from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from uuid import UUID

_cause: ContextVar[tuple[int, UUID | None] | None] = ContextVar(
    "django_domain_events_cause", default=None
)


@contextmanager
def caused_by(event_id: int, correlation_id: UUID | None = None) -> Generator[None]:
    """Mark events fired inside this block as descended from ``event_id``."""
    token = _cause.set((event_id, correlation_id))
    try:
        yield
    finally:
        _cause.reset(token)


def causing_event_id() -> int | None:
    """The event whose receiver is running right now, if any."""
    cause = _cause.get()
    return None if cause is None else cause[0]


def inherited_correlation_id() -> UUID | None:
    """The chain the running receiver belongs to, if any."""
    cause = _cause.get()
    return None if cause is None else cause[1]
