"""Helpers shared across more than one module in this package."""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any, cast

from django.apps import apps


def label_for(module: str, fallback_name: str) -> str:
    """Build a stable ``<app_label>.<name>`` identity for a declaration.

    The app label rather than the dotted import path, because the path is a
    refactor away from orphaning every pending row that names it, and moving a
    module between packages is a change people make without thinking about an
    outbox.

    Refuses rather than guessing when the module belongs to no installed app:
    a made-up label would be written onto rows and only surface as a mismatch
    much later.
    """
    config = apps.get_containing_app_config(module)
    if config is None:
        raise LookupError(
            f"{module} is not inside an installed app, so no stable name can be "
            f"derived for {fallback_name!r}. Pass an explicit name= (events) or "
            f"key= (receivers), or move the declaration into an app in "
            f"INSTALLED_APPS."
        )
    return f"{config.label}.{fallback_name}"


def require_frozen_dataclass(event_class: type) -> None:
    """Reject an event class that cannot behave like a recorded value.

    Frozen because an event is a statement about something that already
    happened: a receiver that mutates one is editing history, and with
    at-least-once delivery it would be editing a different copy on every
    attempt.
    """
    if not dataclasses.is_dataclass(event_class):
        raise TypeError(
            f"{event_class.__name__} is not a dataclass. Events are declared as "
            f"frozen dataclasses so they can be encoded to a payload and rebuilt "
            f"from one."
        )
    # ``@dataclass`` writes __dataclass_params__ at runtime; the stdlib's
    # DataclassInstance protocol does not describe it, so the checker cannot see
    # what is_dataclass() above has already established.
    if not cast(Any, event_class).__dataclass_params__.frozen:
        raise TypeError(
            f"{event_class.__name__} is a mutable dataclass. Declare it "
            f"frozen=True: an event records something that already happened, and "
            f"at-least-once delivery hands a different instance to every attempt, "
            f"so a receiver mutating one is writing to a copy."
        )


def parse_datetime(value: str) -> datetime:
    """Parse a datetime the way ``DjangoJSONEncoder`` writes one.

    The encoder emits UTC with a trailing ``Z``, and ``datetime.fromisoformat``
    only learned to read that in Python 3.11. On this package's floor of 3.10 the
    bare parser raises ``Invalid isoformat string``, so every UTC datetime in a
    payload would fail to decode -- on the oldest supported interpreter only,
    which is the configuration least likely to be the one anybody develops on.

    Found by the version matrix rather than by review: the suite was green on
    3.14 and red on 3.10, 3.11 and 3.12 for exactly this. The two halves of a
    round trip have to agree, and they only agreed on the newest Python.
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
