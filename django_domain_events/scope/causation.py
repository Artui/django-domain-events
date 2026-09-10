from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from uuid import UUID

_cause: ContextVar[tuple[int, UUID | None] | None] = ContextVar(
    "django_domain_events_cause", default=None
)


@contextmanager
def caused_by(event_id: int, correlation_id: UUID | None = None) -> Iterator[None]:
    """Mark events fired inside this block as descended from ``event_id``.

    Set around every receiver, at every execution site, so an event a receiver
    fires records its parent with no ceremony at the call site - a parameter
    threaded through every receiver is a parameter someone forgets.

    Both values come off the parent's row, never from a ``ContextVar``: by the
    time a durable delivery runs, the block that attributed the parent has long
    exited, possibly in another process. Causation is one hop; the correlation
    id is the whole tree, and carrying it here is what keeps a grandchild fired
    hours later in the same chain as the request that started it.
    """
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
