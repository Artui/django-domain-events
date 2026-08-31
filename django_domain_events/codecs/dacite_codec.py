"""The codec for payloads the default one refuses: nested, and richer."""

from __future__ import annotations

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
    cast=[Enum],
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
    """Encode as the default codec does; decode with dacite.

    The schema rule falls out of dacite's own behaviour: a field added with a
    default decodes from an old row, a field removed decodes too, and the two
    breaking changes fail naming the field - which is what lands in
    ``last_error``.

    The ``dacite`` import is bare rather than guarded; a missing extra is
    reported at startup by the codec system check instead.
    """

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E:
        return dacite.from_dict(data_class=event_class, data=payload, config=_CONFIG)
