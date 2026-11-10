from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

import pytest

from django_domain_events.codecs.dacite_codec import DaciteCodec
from tests.testapp.events import Currency, OrderPlaced

codec = DaciteCodec()


@dataclass(frozen=True, slots=True)
class Line:
    sku: str
    qty: int


@dataclass(frozen=True, slots=True)
class Basket:
    """The shape the default codec refuses: nested, and a list of nested."""

    basket_id: int
    total: Decimal
    opened_at: datetime
    trace: UUID
    currency: Currency
    kind: Literal["retail", "wholesale"]
    lines: list[Line]
    note: str | None = None


def _basket() -> Basket:
    return Basket(
        basket_id=7,
        total=Decimal("19.99"),
        opened_at=datetime(2026, 8, 31, 9, tzinfo=timezone.utc),
        trace=uuid4(),
        currency=Currency.EUR,
        kind="retail",
        lines=[Line("abc", 2)],
    )


def test_a_nested_payload_round_trips() -> None:
    """What the extra dependency buys: the default codec already encodes this correctly."""
    value = _basket()
    assert codec.decode(Basket, codec.encode(value), 1) == value


def test_nested_items_rebuild_as_their_class_not_as_dicts() -> None:
    value = _basket()
    rebuilt = codec.decode(Basket, codec.encode(value), 1)
    assert isinstance(rebuilt.lines[0], Line)


def test_a_field_added_with_a_default_decodes_from_an_older_row() -> None:
    payload = codec.encode(_basket())
    del payload["note"]
    assert codec.decode(Basket, payload, 1).note is None


def test_a_field_removed_from_the_class_is_ignored() -> None:
    """Non-strict is load-bearing: strict would dead-letter a removed field."""
    value = _basket()
    payload = {**codec.encode(value), "retired": 1}
    assert codec.decode(Basket, payload, 1) == value


def test_a_required_field_missing_names_the_field() -> None:
    """The breaking change fails with a sentence an operator can read as the reason."""
    payload = codec.encode(_basket())
    del payload["basket_id"]
    with pytest.raises(Exception, match="basket_id"):
        codec.decode(Basket, payload, 1)


def test_a_wrong_type_names_the_field_and_what_was_expected() -> None:
    payload = {**codec.encode(_basket()), "basket_id": "seven"}
    with pytest.raises(Exception, match="basket_id"):
        codec.decode(Basket, payload, 1)


def test_a_literal_outside_its_options_is_rejected() -> None:
    payload = {**codec.encode(_basket()), "kind": "nope"}
    with pytest.raises(Exception, match="kind"):
        codec.decode(Basket, payload, 1)


def test_it_also_handles_what_the_default_codec_handles(order: OrderPlaced) -> None:
    """Swapping the codec must not narrow anything, or the choice would not be a seam."""
    assert codec.decode(OrderPlaced, codec.encode(order), 1) == order
