from __future__ import annotations

import dataclasses
import types
import typing
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, TypeVar
from uuid import UUID

import dacite

from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.utils import parse_datetime

E = TypeVar("E")

# Without the hooks a bare from_dict rejects the strings DjangoJSONEncoder
# wrote. strict stays off deliberately: it is what lets a row written before a
# field was removed still decode, instead of becoming a dead letter.
_CONFIG = dacite.Config(
    # ``tuple`` is cast rather than hooked because dacite type-checks against
    # the annotation and JSON only ever produced a list: without it a
    # ``tuple[X, ...]`` field -- the idiomatic sequence for the frozen
    # dataclass ``@event`` requires -- fails with WrongTypeError on decode,
    # having encoded and committed happily.
    cast=[Enum, tuple],
    type_hooks={
        Decimal: Decimal,
        UUID: UUID,
        datetime: parse_datetime,
        date: date.fromisoformat,
        time: time.fromisoformat,
    },
    strict=False,
)


class DaciteCodec(DataclassCodec):
    """Encode as the default codec does; decode with dacite, so additive changes still load."""

    def supported_annotation(self, annotation: Any) -> bool:
        """Everything the flat codec takes, plus the nesting this codec exists for."""
        if dataclasses.is_dataclass(annotation) and isinstance(annotation, type):
            return all(
                self.supported_annotation(hint)
                for hint in typing.get_type_hints(annotation).values()
            )
        origin = typing.get_origin(annotation)
        if origin in (list, tuple, typing.Union, types.UnionType):
            args = [a for a in typing.get_args(annotation) if a is not Ellipsis]
            return all(self.supported_annotation(a) for a in args if a is not type(None))
        return super().supported_annotation(annotation)

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E:
        return dacite.from_dict(data_class=event_class, data=payload, config=_CONFIG)
