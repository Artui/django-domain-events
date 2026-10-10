from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import TypeVar, overload

from django_domain_events.declaration.registry import registry
from django_domain_events.types.registered_event import RegisteredEvent
from django_domain_events.types.retention import Retention
from django_domain_events.utils import (
    label_for,
    require_frozen_dataclass,
    require_valid_upgrade,
)

E = TypeVar("E", bound=type)

_LONGEST_RETENTION = timedelta(seconds=2**31 - 1)
"""The most ``EventRecord.retention_seconds`` holds: a signed 32-bit integer
on Postgres, about 68 years. SQLite stores more, so a longer window would pass
every test there and then fail every ``fire()`` on Postgres, inside the
caller's transaction."""


@overload
def event(cls: E) -> E: ...
@overload
def event(
    *, name: str | None = None, version: int = 1, retention: timedelta | Retention | None = None
) -> Callable[[E], E]: ...
def event(
    cls: type | None = None,
    *,
    name: str | None = None,
    version: int = 1,
    retention: timedelta | Retention | None = None,
) -> type | Callable[[type], type]:
    """Register a frozen dataclass as an event, bare or called.

    The default name is ``<app_label>.<ClassName>``. Pin it with ``name=`` when
    renaming the class would otherwise strand rows written under the old one.

    ``retention`` says how long ``prune_events`` keeps this event's rows: a
    ``timedelta`` for a window of its own, ``Retention.SUCCEEDED`` or
    ``Retention.SETTLED`` to delete it once consumed, or None for
    ``RETENTION_DAYS``. ``fire()`` copies it onto every row it records, so a
    later change here does not reach events already recorded.
    """
    _require_valid_retention(retention)

    def decorate(target: type) -> type:
        require_frozen_dataclass(target)
        require_valid_upgrade(target)
        resolved = name if name is not None else label_for(target.__module__, target.__name__)
        registry.register_event(
            RegisteredEvent(event_class=target, name=resolved, version=version, retention=retention)
        )
        return target

    if cls is not None:
        return decorate(cls)
    return decorate


def _require_valid_retention(retention: object) -> None:
    """Refuse a retention the prune could not honour, at declaration.

    Here rather than at ``fire()``, because a declaration is imported at
    startup and a ``fire()`` runs inside somebody's request. A bare number is
    refused rather than read as seconds or days, since it means either.

    The window guard is one branch arc of three conditions, each held by cases
    of ``test_a_window_the_column_cannot_hold_is_refused``: zero, a negative
    window and half a second by the lower bound (half a second would be stored
    as zero, which deletes at once), a second and a half by the whole-seconds
    test (stored as one, so not what was declared), and 2**31 seconds by the
    upper bound.
    """
    if retention is None or isinstance(retention, Retention):
        return
    if not isinstance(retention, timedelta):
        raise TypeError(
            f"retention={retention!r} is not a timedelta or a Retention. Pass "
            f"timedelta(days=...) for a window of its own, or Retention.SUCCEEDED "
            f"or Retention.SETTLED to delete the event once it is consumed."
        )
    if (
        retention < timedelta(seconds=1)
        or retention % timedelta(seconds=1)
        or retention > _LONGEST_RETENTION
    ):
        raise ValueError(
            f"retention={retention!r} is not a whole number of seconds between one "
            f"second and {_LONGEST_RETENTION.days} days, which is what an event row "
            f"can record."
        )
