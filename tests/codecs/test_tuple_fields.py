"""Tuple fields, which ``@event``'s own frozen-dataclass rule steers callers towards.

A ``list`` field in a frozen dataclass is a mutable field in a nominally
immutable object, so ``tuple`` is the idiomatic sequence type for an event
payload -- and until this was fixed, neither codec could read one back. JSON has
no tuple, so the encoder writes a list either way; what was missing was the
decoder agreeing about the type it had asked for.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from django_domain_events.codecs.dacite_codec import DaciteCodec
from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType

plain = DataclassCodec()
nested = DaciteCodec()


@dataclass(frozen=True, slots=True)
class Seat:
    section: str
    number: int


@dataclass(frozen=True, slots=True)
class ScalarTuple:
    ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class FixedTuple:
    point: tuple[int, str]


@dataclass(frozen=True, slots=True)
class NestedTuple:
    seats: tuple[Seat, ...]


@dataclass(frozen=True, slots=True)
class BareTuple:
    anything: tuple


def test_a_variadic_tuple_of_scalars_round_trips() -> None:
    """The case the frozen-dataclass rule produces most often."""
    event = ScalarTuple(ids=(1, 2, 3))

    payload = plain.encode(event)

    assert payload["ids"] == [1, 2, 3], "JSON has no tuple, so a list is correct on the wire"
    assert plain.decode(ScalarTuple, payload, 1) == event


def test_a_fixed_length_tuple_round_trips() -> None:
    event = FixedTuple(point=(4, "north"))

    assert plain.decode(FixedTuple, plain.encode(event), 1) == event


def test_a_fixed_length_tuple_refuses_the_wrong_length() -> None:
    """Refused rather than truncated: a row of the wrong shape is a defect, and
    silently dropping the extra would make the payload disagree with itself."""
    with pytest.raises(UnsupportedPayloadType, match="expects 2"):
        plain.decode(FixedTuple, {"point": [4, "north", "extra"]}, 1)


def test_a_bare_tuple_is_still_refused() -> None:
    """Unchanged: with no item type there is nothing to rebuild the items as."""
    with pytest.raises(UnsupportedPayloadType, match="anything"):
        plain.decode(BareTuple, {"anything": [1, 2]}, 1)


def test_the_flat_codec_still_refuses_a_tuple_of_dataclasses() -> None:
    """Also unchanged, and the hint it gives is now correct for this case."""
    with pytest.raises(UnsupportedPayloadType, match="dacite"):
        plain.decode(NestedTuple, {"seats": [{"section": "A", "number": 1}]}, 1)


def test_dacite_decodes_a_tuple_of_scalars() -> None:
    event = ScalarTuple(ids=(1, 2, 3))

    assert nested.decode(ScalarTuple, nested.encode(event), 1) == event


def test_dacite_decodes_a_tuple_of_dataclasses() -> None:
    """The payload the consumer that found this was actually firing."""
    event = NestedTuple(seats=(Seat("STALLS", 1), Seat("CIRCLE", 9)))

    decoded = nested.decode(NestedTuple, nested.encode(event), 1)

    assert decoded == event
    assert isinstance(decoded.seats, tuple)
    assert decoded.seats[1].section == "CIRCLE"
