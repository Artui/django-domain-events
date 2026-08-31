from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any, cast

from django.apps import apps


def label_for(module: str, fallback_name: str) -> str:
    """Build a ``<app_label>.<name>`` identity for a declaration.

    The app label rather than the dotted import path: the path is a refactor
    away from orphaning every pending row that names it.
    """
    config = apps.get_containing_app_config(module)
    if config is None:
        raise LookupError(
            f"{module} is not inside an installed app, so no stable name can be "
            f"derived for {fallback_name!r}. Pass an explicit name= or key=."
        )
    return f"{config.label}.{fallback_name}"


def require_frozen_dataclass(event_class: type) -> None:
    """Reject an event class that cannot behave like a recorded value."""
    if not dataclasses.is_dataclass(event_class):
        raise TypeError(
            f"{event_class.__name__} is not a dataclass. Events are declared as "
            f"frozen dataclasses so they can be encoded and rebuilt."
        )
    # __dataclass_params__ is written at runtime and absent from the stdlib
    # protocol, so the checker cannot see what is_dataclass has established.
    if not cast(Any, event_class).__dataclass_params__.frozen:
        raise TypeError(
            f"{event_class.__name__} is a mutable dataclass. Declare it "
            f"frozen=True: at-least-once delivery hands a different instance to "
            f"every attempt, so a receiver mutating one writes to a copy."
        )


def parse_datetime(value: str) -> datetime:
    """Parse a datetime the way ``DjangoJSONEncoder`` writes one.

    The encoder emits UTC with a trailing ``Z``, which ``fromisoformat`` only
    learned to read in Python 3.11. Bare parsing fails on the 3.10 floor.
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
