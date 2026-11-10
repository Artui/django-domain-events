from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class _Suppression:
    classes: tuple[type, ...]
    reason: str
    record: bool


_stack: ContextVar[tuple[_Suppression, ...]] = ContextVar(
    "django_domain_events_suppressed", default=()
)


@contextmanager
def suppressed(*event_classes: type, reason: str, record: bool = True) -> Generator[None]:
    """Fire these events without delivering them, and say why."""
    if not reason:
        raise ValueError(
            "suppressed() needs a reason. It lands on the row, and an event "
            "dropped without one is indistinguishable from a bug."
        )
    from django_domain_events.models.event_record import EventRecord

    limit = EventRecord.suppressed_reason.field.max_length
    assert limit is not None
    if len(reason) > limit:
        raise ValueError(
            f"suppressed() was given a reason of {len(reason)} characters; the limit is "
            f"{limit}, the width of the column the event row stores it in. A longer one "
            f"is rejected by the database on Postgres and stored whole on SQLite, so it "
            f"would work in tests and fail in production. Shorten the reason."
        )
    for candidate in event_classes:
        if not isinstance(candidate, type):
            raise TypeError(
                f"suppressed() takes event classes, not {candidate!r}. Passing an "
                f"instance or a name fails much later, inside fire()."
            )
    token = _stack.set((*_stack.get(), _Suppression(event_classes, reason, record)))
    try:
        yield
    finally:
        _stack.reset(token)


def suppression_for(event_class: type) -> tuple[str, bool] | None:
    """The reason and the record flag if this class is suppressed, else None."""
    matches = [
        s
        for s in _stack.get()
        if not s.classes or any(issubclass(event_class, c) for c in s.classes)
    ]
    if not matches:
        return None
    return matches[-1].reason, all(s.record for s in matches)
