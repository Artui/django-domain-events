from __future__ import annotations

from collections.abc import Iterator
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
def suppressed(*event_classes: type, reason: str, record: bool = True) -> Iterator[None]:
    """Fire these events without delivering them, and say why.

    The reason is required and lands on the row. A silently dropped event is
    unauditable, which is the failure mode suppression is most likely to cause,
    so the default writes the event and marks it rather than discarding it.

    ``record=False`` discards instead. It exists because a hundred-thousand-row
    import writing a hundred thousand suppressed rows is a surprise, and it
    trades the audit trail for the write - which is the whole point of the
    default, so it is named rather than defaulted.

    Nested blocks accumulate rather than replace. A library that suppresses its
    own event type inside your block must not re-enable yours: the innermost
    matching reason is the one recorded, and any matching block asking not to
    record wins, because that is the safer half of the disagreement.
    """
    if not reason:
        raise ValueError(
            "suppressed() needs a reason. It lands on the row, and an event "
            "dropped without one is indistinguishable from a bug."
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
