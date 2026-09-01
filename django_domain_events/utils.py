from __future__ import annotations

import dataclasses
import inspect
from collections.abc import Mapping
from datetime import datetime
from typing import Any, cast

from django.apps import apps
from django.db import connections

from django_domain_events.payload_upgrade_failed import PayloadUpgradeFailed
from django_domain_events.registry import registry
from django_domain_events.types.delivery_status import DeliveryStatus

TERMINAL = (DeliveryStatus.SUCCEEDED, DeliveryStatus.DEAD, DeliveryStatus.ORPHANED)
"""Statuses a delivery does not come back from.

Everything else is still owed. Named once because three callers ask the
question and an explicit list of the owed statuses is what let two of them
disagree: one omitted CLAIMED, so a row whose worker died between the claim
and the deploy that deleted its receiver read as settled until a relay
happened to reclaim it.
"""


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


def has_table(alias: str, table: str) -> bool:
    """Whether a table exists on one connection.

    Both database checks need it. Without it they run under ``migrate`` - which
    passes a database - and query a table ``migrate`` has not created yet, so
    the first command a new project runs dies and no tables are created at all.
    """
    connection = connections[alias]
    with connection.cursor() as cursor:
        return table in connection.introspection.table_names(cursor)


def require_valid_upgrade(event_class: type) -> None:
    """Reject an ``upgrade`` hook that cannot be called the way we call it.

    At the decorator rather than in the relay hours later, which is the same
    reason the frozen check is here. A plain ``def upgrade(self, ...)`` is the
    trap worth naming: reached through the class it is an unbound function, so
    the payload would silently arrive as ``self``.
    """
    hook = inspect.getattr_static(event_class, "upgrade", None)
    if hook is None:
        return
    if not isinstance(hook, (staticmethod, classmethod)):
        raise TypeError(
            f"{event_class.__name__}.upgrade must be a staticmethod or a "
            f"classmethod. Reached through the class an ordinary method is "
            f"unbound, so the payload would arrive as its first argument."
        )
    try:
        inspect.signature(event_class.upgrade).bind({}, 1)
    except TypeError as exc:
        raise TypeError(
            f"{event_class.__name__}.upgrade must accept (payload, from_version): {exc}"
        ) from exc


def decode_payload(event_class: type, payload: dict[str, Any], version: int) -> Any:
    """Rebuild an event from a row, migrating an older payload first.

    Shared by the relay and by ``assert_fired`` so the two cannot decode the
    same row differently - a test helper that reads a payload the relay would
    reject is how a suite comes to agree with a bug.

    The hook runs only when the row is *older* than the declaration. A row from
    the future is a rollback, and no forward migration helps: the code that
    would know how to read it is the code that was just removed.
    """
    # Function-local, and only this one: settings imports the codec package,
    # which imports this module. registry has no such cycle and is imported at
    # the top.
    from django_domain_events.settings import get_codec

    entry = registry.event_for_class(event_class)
    hook = getattr(event_class, "upgrade", None)
    if hook is not None and entry is not None and version < entry.version:
        try:
            migrated = hook(payload, version)
        except Exception as exc:
            raise PayloadUpgradeFailed(
                f"{event_class.__name__}.upgrade() failed on a version {version} "
                f"payload: {type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(migrated, Mapping):
            # The commonest way to write the hook wrong is to mutate in place
            # and forget the return. Saying so here is the difference between
            # naming the hook and dead-lettering with "argument of type
            # 'NoneType' is not iterable".
            raise PayloadUpgradeFailed(
                f"{event_class.__name__}.upgrade() returned {type(migrated).__name__}, "
                f"not a mapping. It has to return the migrated payload; mutating "
                f"the one it was given is not enough."
            )
        # The declared version, not the row's: the payload the codec is about
        # to see has been migrated, and a codec is handed the version so it can
        # branch on it. Telling it the old one asks for the old treatment of
        # new-shaped data.
        return get_codec().decode(event_class, dict(migrated), entry.version)
    return get_codec().decode(event_class, payload, version)
