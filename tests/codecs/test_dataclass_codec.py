from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

import pytest

from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType
from tests.testapp.events import OrderPlaced

codec = DataclassCodec()


@dataclass(frozen=True)
class Line:
    sku: str


@dataclass(frozen=True)
class Basket:
    line: Line


@dataclass(frozen=True)
class Lines:
    lines: list[Line]


def test_every_documented_scalar_round_trips(order: OrderPlaced) -> None:
    """The codec claim as one round trip: encode and decode must agree on the table."""
    payload = codec.encode(order)
    assert payload["total"] == "19.99"
    assert payload["currency"] == "EUR"
    assert codec.decode(OrderPlaced, payload, 1) == order


def test_dates_and_times_rebuild_from_their_string_forms() -> None:
    @dataclass(frozen=True)
    class Stamped:
        when: datetime
        day: date
        clock: time

    value = Stamped(
        when=datetime(2026, 8, 31, 9, tzinfo=timezone.utc),
        day=date(2026, 8, 31),
        clock=time(9, 30),
    )
    assert codec.decode(Stamped, codec.encode(value), 1) == value


def test_a_field_added_with_a_default_decodes_from_an_older_row(
    order: OrderPlaced,
) -> None:
    """The tolerant half of the schema rule: the constructor supplies the default."""
    payload = codec.encode(order)
    del payload["note"]
    assert codec.decode(OrderPlaced, payload, 1).note is None


def test_a_field_removed_from_the_class_is_ignored(order: OrderPlaced) -> None:
    """The other tolerant half: an extra key on an old row is not an error."""
    payload = {**codec.encode(order), "retired_field": 1}
    assert codec.decode(OrderPlaced, payload, 1) == order


def test_a_required_field_missing_names_itself(order: OrderPlaced) -> None:
    """The breaking change fails loudly, and its message lands in last_error."""
    payload = codec.encode(order)
    del payload["order_id"]
    with pytest.raises(TypeError, match="order_id"):
        codec.decode(OrderPlaced, payload, 1)


def test_a_literal_outside_its_options_refuses(order: OrderPlaced) -> None:
    """A hand-edited row must not produce an instance its own annotation forbids."""
    payload = {**codec.encode(order), "kind": "wholesale-ish"}
    with pytest.raises(UnsupportedPayloadType, match="is not one of"):
        codec.decode(OrderPlaced, payload, 1)


def test_an_unknown_enum_member_refuses(order: OrderPlaced) -> None:
    payload = {**codec.encode(order), "currency": "GBP"}
    with pytest.raises(ValueError, match="GBP"):
        codec.decode(OrderPlaced, payload, 1)


def test_optional_carries_none_through() -> None:
    @dataclass(frozen=True)
    class Maybe:
        value: Decimal | None

    assert codec.decode(Maybe, {"value": None}, 1).value is None
    assert codec.decode(Maybe, {"value": "1.5"}, 1).value == Decimal("1.5")


def test_a_uuid_rebuilds_as_a_uuid() -> None:
    @dataclass(frozen=True)
    class Traced:
        trace: UUID

    value = Traced(trace=uuid4())
    assert codec.decode(Traced, codec.encode(value), 1) == value


def test_a_nested_dataclass_refuses_and_names_the_codec_that_handles_it() -> None:
    """Refuse rather than approximate."""

    with pytest.raises(UnsupportedPayloadType, match="DaciteCodec"):
        codec.decode(Basket, {"line": {"sku": "abc"}}, 1)


def test_a_list_of_an_unsupported_type_refuses() -> None:
    with pytest.raises(UnsupportedPayloadType, match="lines"):
        codec.decode(Lines, {"lines": [{"sku": "abc"}]}, 1)


def test_a_union_of_two_real_types_refuses() -> None:
    """Optional is a union with None and is supported; a genuine either-or is not."""

    @dataclass(frozen=True)
    class Either:
        value: int | str

    with pytest.raises(UnsupportedPayloadType, match="value"):
        codec.decode(Either, {"value": 1}, 1)


def test_the_refusal_names_the_version_it_choked_on() -> None:
    """An operator reading last_error needs the payload vintage that failed."""

    @dataclass(frozen=True)
    class Odd:
        value: complex

    with pytest.raises(UnsupportedPayloadType, match="version 7"):
        codec.decode(Odd, {"value": 1}, 7)


def test_annotations_that_cannot_be_resolved_say_why() -> None:
    """A function-local field type cannot resolve: only module globals are searched."""

    @dataclass(frozen=True)
    class LocalPart:
        sku: str

    @dataclass(frozen=True)
    class LocalWhole:
        part: LocalPart

    with pytest.raises(UnsupportedPayloadType, match="module level"):
        codec.decode(LocalWhole, {"part": {"sku": "abc"}}, 1)


def test_an_annotation_with_no_class_to_rebuild_refuses() -> None:
    """A parameterised generic is not a class, so there is nothing to rebuild the value as."""

    @dataclass(frozen=True)
    class Mapping:
        value: dict[str, int]

    with pytest.raises(UnsupportedPayloadType, match="value"):
        codec.decode(Mapping, {"value": {"a": 1}}, 1)


def test_any_refuses_too() -> None:
    """Any reaches the refusal by the other route, and must still refuse."""

    @dataclass(frozen=True)
    class Loose:
        value: Any

    with pytest.raises(UnsupportedPayloadType, match="value"):
        codec.decode(Loose, {"value": 1}, 1)


class Colour(Enum):
    """A plain Enum, not the str-mixin kind the fixtures happen to use."""

    RED = "red"


@dataclass(frozen=True)
class Painted:
    colour: Colour


@dataclass(frozen=True)
class Stamped:
    at: datetime
    clock: time


def test_a_plain_enum_round_trips() -> None:
    """The encode side had no Enum branch, so only str-mixin enums survived."""
    value = Painted(colour=Colour.RED)
    assert codec.encode(value) == {"colour": "red"}
    assert codec.decode(Painted, codec.encode(value), 1) == value


def test_microseconds_survive_the_round_trip() -> None:
    """DjangoJSONEncoder truncates to milliseconds, silently changing the event's data."""
    value = Stamped(
        at=datetime(2026, 8, 31, 12, 0, 0, 123456, tzinfo=timezone.utc),
        clock=time(12, 0, 0, 123456),
    )
    assert codec.decode(Stamped, codec.encode(value), 1) == value


def test_a_value_that_cannot_be_written_names_its_field() -> None:
    """The raw json error names no field, and it aborts the caller's transaction."""

    @dataclass(frozen=True)
    class Bad:
        ok: int
        payload: Any

    with pytest.raises(UnsupportedPayloadType, match=r"Bad\.payload"):
        codec.encode(Bad(ok=1, payload={object()}))


def test_the_refusal_falls_back_when_no_single_field_is_at_fault() -> None:
    """The per-field search is a best effort at naming the culprit."""
    from django_domain_events.codecs.dataclass_codec import _unencodable

    problem = _unencodable(Painted(colour=Colour.RED), {}, TypeError("nope"))
    assert isinstance(problem, UnsupportedPayloadType)
    assert "Painted" in str(problem)
