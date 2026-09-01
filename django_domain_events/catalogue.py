from __future__ import annotations

import dataclasses
import inspect
import typing
from collections.abc import Callable
from typing import Any, cast

from django_domain_events.registry import registry
from django_domain_events.types.catalogue import Catalogue
from django_domain_events.types.catalogue_event import CatalogueEvent
from django_domain_events.types.catalogue_field import CatalogueField
from django_domain_events.types.catalogue_receiver import CatalogueReceiver


def catalogue() -> Catalogue:
    """Describe every declared event, its payload shape and its receivers.

    The artefact a team wants and signals cannot produce: what exists, what
    listens, and under which guarantee - generated from the declarations rather
    than maintained beside them, so it cannot drift.

    Sorted by name throughout. A catalogue is written to a file and diffed
    against the last one, and import order is not a difference.
    """
    events = []
    for entry in sorted(registry.events(), key=lambda e: e.name):
        cls = entry.event_class
        receivers = tuple(
            CatalogueReceiver(
                key=r.key,
                callable_path=_callable_path(r.func),
                mode=r.mode.value,
                site=r.site,
                max_attempts=r.max_attempts,
                eager=r.eager,
                takes_context=r.takes_context,
                lease_seconds=r.lease_seconds,
            )
            for r in sorted(registry.receivers_for(cls), key=lambda r: r.key)
        )
        events.append(
            CatalogueEvent(
                name=entry.name,
                version=entry.version,
                class_path=f"{cls.__module__}.{cls.__qualname__}",
                doc=_doc(cls),
                migrates_older_rows=hasattr(cls, "upgrade"),
                fields=_fields(cls),
                receivers=receivers,
            )
        )
    return Catalogue(events=tuple(events))


def _doc(cls: type) -> str:
    """The class docstring, or empty when the dataclass machinery wrote it.

    ``@dataclass`` fills ``__doc__`` with the signature whenever the class has
    none of its own - a subclass of a documented base included, because a class
    always carries its own ``__doc__`` and never inherits one. Reading it
    naively puts ``OrderPlaced(order_id: int)`` in the catalogue where a
    description belongs.
    """
    doc = cls.__doc__
    if not doc or doc.startswith(f"{cls.__name__}("):
        return ""
    return inspect.cleandoc(doc)


def _fields(cls: type) -> tuple[CatalogueField, ...]:
    """Payload fields with their annotations resolved where possible.

    ``get_type_hints`` needs every name in the annotations to be importable at
    runtime, which a consumer with ``if TYPE_CHECKING:`` imports has already
    made false. Falling back to the unresolved annotation keeps the catalogue
    generating for that consumer rather than failing on their behalf.
    """
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
        # cast because @event has already established that cls is a
        # dataclass, which the checker cannot see through a bare `type`.
        for field in dataclasses.fields(cast(Any, cls))
    )


def _callable_path(func: Callable[..., Any]) -> str:
    """Where a receiver's code lives, for a reader who has to go find it.

    Built with getattr rather than attribute access because a receiver is only
    required to be callable: a functools.partial or an instance with __call__
    has neither name.
    """
    module = getattr(func, "__module__", None)
    name = getattr(func, "__qualname__", None) or type(func).__name__
    return f"{module}.{name}" if module else name


def _type_name(annotation: object) -> str:
    """Render an annotation the way a reader of the catalogue needs it.

    A parameterised generic is rendered with str() and a plain class by name,
    because the reverse loses exactly the information worth publishing:
    ``list[str]`` reports its ``__name__`` as ``list`` and
    ``Literal["retail", "wholesale"]`` reports ``Literal``, so a catalogue built
    on ``__name__`` alone says a field is a list without saying of what.
    """
    if isinstance(annotation, str):
        # An unresolved annotation reaches here as source text, so a consumer
        # who quoted the forward reference would otherwise be published with
        # the quotes. Both spellings mean the same thing and must render the
        # same way.
        return annotation.strip("'\"")
    if typing.get_origin(annotation) is not None:
        return str(annotation)
    return getattr(annotation, "__name__", None) or str(annotation)


def _default(field: dataclasses.Field[Any]) -> str | None:
    if field.default is not dataclasses.MISSING:
        return repr(field.default)
    if field.default_factory is not dataclasses.MISSING:
        # Named, not called: a factory is arbitrary consumer code, and building
        # a catalogue must not run it.
        return f"{getattr(field.default_factory, '__name__', 'factory')}()"
    return None
