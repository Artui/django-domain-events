from __future__ import annotations

import dataclasses
import inspect
import typing
from collections.abc import Callable, Iterable
from datetime import timedelta
from typing import Any, cast

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.registry import registry
from django_domain_events.types.catalogue import Catalogue
from django_domain_events.types.catalogue_event import CatalogueEvent
from django_domain_events.types.catalogue_field import CatalogueField
from django_domain_events.types.catalogue_receiver import CatalogueReceiver
from django_domain_events.types.registered_receiver import RegisteredReceiver


def catalogue() -> Catalogue:
    """Describe every declared event, its payload shape and its receivers.

    The artefact a team wants and signals cannot produce: what exists, what
    listens, and under which guarantee - generated from the declarations rather
    than maintained beside them, so it cannot drift.

    Sorted by name throughout. A catalogue is written to a file and diffed
    against the last one, and import order is not a difference.

    Wildcard receivers are listed once, in their own section, and not under
    each event they receive - which is every event.
    """
    events = []
    for entry in sorted(registry.events(), key=lambda e: e.name):
        cls = entry.event_class
        receivers = _described(r for r in registry.receivers() if r.event_class is cls)
        events.append(
            CatalogueEvent(
                name=entry.name,
                version=entry.version,
                class_path=f"{cls.__module__}.{cls.__qualname__}",
                doc=_doc(cls),
                migrates_older_rows=hasattr(cls, "upgrade"),
                retention_seconds=entry.retention_columns[0],
                delete_when=entry.retention_columns[1],
                fields=_fields(cls),
                receivers=receivers,
            )
        )
    return Catalogue(
        events=tuple(events),
        wildcard_receivers=_described(r for r in registry.receivers() if r.event_class is AnyEvent),
    )


def _described(receivers: Iterable[RegisteredReceiver]) -> tuple[CatalogueReceiver, ...]:
    """Catalogue entries for some receivers, sorted by key."""
    return tuple(
        CatalogueReceiver(
            key=r.key,
            callable_path=_callable_path(r.func),
            mode=r.mode.value,
            site=r.site,
            max_attempts=r.max_attempts,
            eager=r.eager,
            takes_context=r.takes_context,
            lease_seconds=r.lease_seconds,
            targets=None if r.targets is None else _callable_path(r.targets),
            backoff_base_seconds=r.backoff_base_seconds,
            backoff_cap_seconds=r.backoff_cap_seconds,
            lane=r.lane,
            give_up_after_seconds=(
                None if r.give_up_after is None else r.give_up_after.total_seconds()
            ),
            due_seconds=r.due.total_seconds() if isinstance(r.due, timedelta) else None,
            due_callable=_callable_path(r.due) if callable(r.due) else None,
        )
        for r in sorted(receivers, key=lambda r: r.key)
    )


def _doc(cls: type) -> str:
    """The class docstring, or empty when @dataclass wrote it: it fills in the signature."""
    doc = cls.__doc__
    if not doc or doc.startswith(f"{cls.__name__}("):
        return ""
    return inspect.cleandoc(doc)


def _fields(cls: type) -> tuple[CatalogueField, ...]:
    """Payload fields with their annotations resolved where possible, else left unresolved."""
    try:
        hints = typing.get_type_hints(cls)
    except (NameError, TypeError):
        hints = {}
    return tuple(
        CatalogueField(
            name=field.name,
            type=_type_name(hints.get(field.name, field.type)),
            required=field.default is dataclasses.MISSING
            and field.default_factory is dataclasses.MISSING,
            default=_default(field),
        )
        for field in dataclasses.fields(cast(Any, cls))
    )


def _callable_path(func: Callable[..., Any]) -> str:
    """Where a receiver's code lives: getattr, since a partial has no __name__ to read."""
    module = getattr(func, "__module__", None)
    name = getattr(func, "__qualname__", None) or type(func).__name__
    return f"{module}.{name}" if module else name


def _type_name(annotation: object) -> str:
    """Render an annotation as str() for a generic, by name for a class: __name__ loses args."""
    if isinstance(annotation, str):
        return annotation.strip("'\"")
    if typing.get_origin(annotation) is not None:
        return str(annotation)
    return getattr(annotation, "__name__", None) or str(annotation)


def _default(field: dataclasses.Field[Any]) -> str | None:
    if field.default is not dataclasses.MISSING:
        return repr(field.default)
    if field.default_factory is not dataclasses.MISSING:
        # Named, not called: a factory is arbitrary consumer code.
        return f"{getattr(field.default_factory, '__name__', 'factory')}()"
    return None
