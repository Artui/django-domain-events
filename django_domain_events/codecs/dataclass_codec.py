"""The default codec: stdlib only, narrow on purpose, and loud at the edge."""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, TypeVar
from uuid import UUID

from django.core.serializers.json import DjangoJSONEncoder

from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType

E = TypeVar("E")

_SCALARS: tuple[type, ...] = (str, int, float, bool, Decimal, UUID, datetime, date, time)
"""What this codec will rebuild from JSON without a dependency.

Deliberately the set ``DjangoJSONEncoder`` already knows how to write, so the
two halves of the round trip are defined by the same table.
"""

_PARSERS: dict[type, Any] = {
    Decimal: Decimal,
    UUID: UUID,
    datetime: datetime.fromisoformat,
    date: date.fromisoformat,
    time: time.fromisoformat,
}
"""Scalars that arrive as strings and need rebuilding. ``str``, ``int``,
``float`` and ``bool`` survive JSON as themselves and are absent by design."""


class DataclassCodec:
    """Flat payloads of documented scalars. Everything else refuses by name.

    The point of the narrow table is that the boundary is legible: a consumer
    either sees their event round-trip, or sees an error naming the field, its
    type, and the codec that handles it. What they never get is a silent
    approximation.
    """

    def encode(self, event: object) -> dict[str, Any]:
        """Encode through Django's own JSON encoder.

        ``asdict`` then a ``json`` round trip rather than hand-mapping: the
        encoder is already a dependency, already handles every scalar in the
        table above, and is the same code Django uses for its own JSON fields.
        """
        return json.loads(
            json.dumps(dataclasses.asdict(typing.cast(Any, event)), cls=DjangoJSONEncoder)
        )

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E:
        """Rebuild an instance, coercing each field to its declared type."""
        try:
            hints = typing.get_type_hints(event_class)
        except NameError as exc:
            # ``from __future__ import annotations`` turns every annotation into
            # a string, and resolving one needs the module globals. A class
            # defined inside a function has none, so its annotations name types
            # nothing can look up. The bare NameError points at the missing name
            # with no hint that the declaration site is the problem.
            raise UnsupportedPayloadType(
                f"{event_class.__name__}: its annotations cannot be resolved "
                f"({exc}). Declare events at module level, where the names they "
                f"reference are importable."
            ) from exc
        kwargs: dict[str, Any] = {}
        for field in _fields_of(event_class):
            if field.name not in payload:
                # Absent is not an error here. A field added with a default is
                # exactly the additive change the schema rule permits, and the
                # dataclass constructor supplies the default. A field added
                # *without* one raises TypeError from the constructor below,
                # which names it.
                continue
            kwargs[field.name] = _coerce(
                payload[field.name], hints[field.name], event_class, field.name, version
            )
        return event_class(**kwargs)


def _fields_of(event_class: type) -> tuple[dataclasses.Field[Any], ...]:
    """Narrow the type for the checker at the one place it cannot see the shape.

    ``@event`` rejects a non-dataclass at declaration time, so by the time a
    codec runs this is known-good. A cast rather than a suppression comment: the
    package type-checks with ty, which does not read mypy pragmas, so a comment
    here would claim a checker that is not running.
    """
    return dataclasses.fields(typing.cast(Any, event_class))


def _coerce(value: Any, annotation: Any, event_class: type, field_name: str, version: int) -> Any:
    """Rebuild one field's value from its JSON form."""
    origin = typing.get_origin(annotation)

    if origin is typing.Literal:
        # Literal members are values, not types, so there is nothing to rebuild.
        # Validating here rather than trusting the payload keeps a hand-edited
        # row from producing an instance its own annotation forbids.
        if value not in typing.get_args(annotation):
            raise UnsupportedPayloadType(
                f"{event_class.__name__}.{field_name} (version {version}): "
                f"{value!r} is not one of {typing.get_args(annotation)}"
            )
        return value

    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if value is None:
            return None
        if len(args) != 1:
            raise _refuse(event_class, field_name, annotation, version)
        return _coerce(value, args[0], event_class, field_name, version)

    if origin is list:
        (item_annotation,) = typing.get_args(annotation)
        return [_coerce(item, item_annotation, event_class, field_name, version) for item in value]

    if isinstance(annotation, type):
        if issubclass(annotation, Enum):
            return annotation(value)
        if annotation in _PARSERS:
            return _PARSERS[annotation](value)
        if annotation in _SCALARS:
            return value

    raise _refuse(event_class, field_name, annotation, version)


def _refuse(
    event_class: type, field_name: str, annotation: Any, version: int
) -> UnsupportedPayloadType:
    """Build the refusal, naming the field and the codec that would handle it."""
    return UnsupportedPayloadType(
        f"{event_class.__name__}.{field_name} (version {version}) is annotated "
        f"{annotation!r}, which DataclassCodec does not rebuild. It handles flat "
        f"payloads of {', '.join(t.__name__ for t in _SCALARS)}, enums, literals, "
        f"optionals and lists of those. For nested dataclasses and richer shapes, "
        f"install the 'dacite' extra and set CODEC to "
        f"'django_domain_events.codecs.dacite_codec.DaciteCodec'."
    )
