from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_suppressed: ContextVar[tuple[tuple[type, ...], str, bool] | None] = ContextVar(
    "django_domain_events_suppressed", default=None
)


@contextmanager
def suppressed(*event_classes: type, reason: str, record: bool = True) -> Iterator[None]:
    """Fire these events without delivering them, and say why.

    The reason is required and lands on the row. A silently dropped event is
    unauditable, which is the failure mode suppression is most likely to cause,
    so the default writes the event and marks it rather than discarding it.

    ``record=False`` discards instead. It exists because a hundred-thousand-row
    import writing a hundred thousand suppressed rows is a surprise, and it
    trades the audit trail for the write - which is the whole point of the
    default, so it is named rather than defaulted.
    """
    if not reason:
        raise ValueError(
            "suppressed() needs a reason. It lands on the row, and an event "
            "dropped without one is indistinguishable from a bug."
        )
    token = _suppressed.set((event_classes, reason, record))
    try:
        yield
    finally:
        _suppressed.reset(token)


def suppression_for(event_class: type) -> tuple[str, bool] | None:
    """The reason and the record flag if this class is suppressed, else None."""
    active = _suppressed.get()
    if active is None:
        return None
    classes, reason, record = active
    if classes and not any(issubclass(event_class, c) for c in classes):
        return None
    return reason, record
