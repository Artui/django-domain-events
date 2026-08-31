"""The codec for payloads the default one refuses: nested, and richer."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, TypeVar
from uuid import UUID

import dacite

from django_domain_events.codecs.dataclass_codec import DataclassCodec

E = TypeVar("E")

_CONFIG = dacite.Config(
    cast=[Enum],
    type_hooks={
        Decimal: Decimal,
        UUID: UUID,
        datetime: datetime.fromisoformat,
        date: date.fromisoformat,
        time: time.fromisoformat,
    },
    # strict stays OFF, and that is load-bearing rather than a default left
    # alone. Non-strict is what lets a row written before a field was *removed*
    # still decode: the extra key is ignored instead of raising. Turning it on
    # would convert the tolerant half of the schema rule into a dead letter.
    strict=False,
)
"""Decode configuration.

Without the hooks a bare ``from_dict`` rejects a ``Decimal`` arriving as the
string ``DjangoJSONEncoder`` wrote, so the two halves of the round trip only
agree because this object exists. It is owned here rather than by each consumer
for exactly that reason.
"""


class DaciteCodec(DataclassCodec):
    """Encode as the default codec does; decode with dacite.

    The ``dacite`` import above is bare rather than wrapped in a friendly
    ``try``. A guard here would only ever fire at the moment the codec is first
    imported, which is halfway through handling a real event; the same problem
    is caught at startup by the ``codec_dependency_is_installed`` system check,
    which is where a missing dependency should be reported. Two mechanisms for
    one failure means the good one has to compete with the noisy one.

    Encoding needs nothing extra, so it is inherited rather than restated. What
    the dependency buys is entirely on the decode side: nested dataclasses,
    ``list[SomeDataclass]``, unions and the rest.

    The schema rule falls out of dacite's own behaviour rather than being
    enforced here. A field added with a default decodes from an old row (the
    default fills in); a field removed decodes too (the extra key is ignored);
    and the two breaking changes fail with a message naming the field, which is
    what lands in ``last_error``.
    """

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E:
        return dacite.from_dict(data_class=event_class, data=payload, config=_CONFIG)
