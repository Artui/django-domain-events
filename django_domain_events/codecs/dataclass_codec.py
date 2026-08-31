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

from django_domain_events.codecs.payload_encoder import PayloadEncoder
from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType
from django_domain_events.utils import parse_datetime

E = TypeVar("E")

# The set PayloadEncoder writes, so both halves of the round trip are defined by
# the same table.
_SCALARS: tuple[type, ...] = (str, int, float, bool, Decimal, UUID, datetime, date, time)

# Scalars that arrive as strings. str/int/float/bool survive JSON as themselves.
_PARSERS: dict[type, Any] = {
    Decimal: Decimal,
    UUID: UUID,
    datetime: parse_datetime,
    date: date.fromisoformat,
    time: time.fromisoformat,
}


class DataclassCodec:
    """Flat payloads of documented scalars. Everything else refuses by name."""

    def encode(self, event: object) -> dict[str, Any]:
        fields = dataclasses.asdict(typing.cast(Any, event))
        try:
            return json.loads(json.dumps(fields, cls=PayloadEncoder))
        except (TypeError, ValueError) as exc:
            raise _unencodable(event, fields, exc) from exc

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E:
        try:
            hints = typing.get_type_hints(event_class)
        except NameError as exc:
            raise UnsupportedPayloadType(
                f"{event_class.__name__}: its annotations cannot be resolved "
                f"({exc}). Declare events at module level, where the names they "
                f"reference are importable."
            ) from exc

        kwargs: dict[str, Any] = {}
        for field in dataclasses.fields(typing.cast(Any, event_class)):
            # Absent is not an error: a field added with a default is the
            # additive change the schema rule permits, and the constructor
            # supplies it. One added without a default raises below, named.
            if field.name in payload:
                kwargs[field.name] = _coerce(
                    payload[field.name], hints[field.name], event_class, field.name, version
                )
        return event_class(**kwargs)


def _coerce(value: Any, annotation: Any, event_class: type, field_name: str, version: int) -> Any:
    origin = typing.get_origin(annotation)

    if origin is typing.Literal:
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
    return UnsupportedPayloadType(
        f"{event_class.__name__}.{field_name} (version {version}) is annotated "
        f"{annotation!r}, which DataclassCodec does not rebuild. It handles flat "
        f"payloads of {', '.join(t.__name__ for t in _SCALARS)}, enums, literals, "
        f"optionals and lists of those. For nested dataclasses, install the "
        f"'dacite' extra and set CODEC to "
        f"'django_domain_events.codecs.dacite_codec.DaciteCodec'."
    )


def _unencodable(event: object, fields: dict[str, Any], exc: Exception) -> UnsupportedPayloadType:
    """Name the field that could not be written.

    The raw error from ``json`` names a type and no field, and it surfaces
    inside the caller's transaction - so it takes the business change down with
    it and says only that some object was not serializable.
    """
    for name, value in fields.items():
        try:
            json.dumps({name: value}, cls=PayloadEncoder)
        except (TypeError, ValueError):
            return UnsupportedPayloadType(
                f"{type(event).__name__}.{name} cannot be written to a payload: "
                f"{type(exc).__name__}: {exc}"
            )
    return UnsupportedPayloadType(f"{type(event).__name__}: {type(exc).__name__}: {exc}")
