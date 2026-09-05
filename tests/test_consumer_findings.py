"""The four findings the first outside consumer reported, pinned.

Grouped in one file because they were one report and because two of them are
the same shape: a declaration that is accepted, recorded, and only refused
somewhere the caller is no longer watching.
"""

from __future__ import annotations

import dataclasses
import typing
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest
from django.db import transaction
from django.test import override_settings

from django_domain_events import checks
from django_domain_events.attributed import attributed
from django_domain_events.codecs.dacite_codec import DaciteCodec
from django_domain_events.codecs.dataclass_codec import DataclassCodec, _coerce
from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType
from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.event import event
from django_domain_events.fire import fire
from django_domain_events.registry import registry
from django_domain_events.types.delivery_context import DeliveryContext
from tests.testapp.events import OrderPlaced


class Colour(Enum):
    RED = "red"


@dataclass(frozen=True, slots=True)
class Seat:
    section: str
    number: int


@dataclass(frozen=True, slots=True)
class HasASet:
    tags: set[str]


@dataclass(frozen=True, slots=True)
class WrapsASet:
    inner: HasASet


class OpaqueCodec:
    """A codec that answers neither question, as a third party's might."""

    def encode(self, event: object) -> dict[str, object]:  # pragma: no cover
        return {}

    def decode(self, event_class: type, payload: dict, version: int) -> object:  # pragma: no cover
        return None


# --------------------------------------------------------------------------
# The predicate and the coercion have to agree, or the check is worse than none
# --------------------------------------------------------------------------

_SUPPORTED: list[tuple[typing.Any, object]] = [
    (int, 1),
    (str, "a"),
    (float, 1.5),
    (bool, True),
    (Decimal, "1.5"),
    (UUID, "3f333df6-90a4-4fda-8dd3-9485d27cee36"),
    (datetime, "2026-09-05T00:00:00+00:00"),
    (date, "2026-09-05"),
    (time, "12:00:00"),
    (Colour, "red"),
    (typing.Literal["a", "b"], "a"),
    (int | None, 1),
    (list[int], [1, 2]),
    (tuple[int, ...], [1, 2]),
    (tuple[int, str], [1, "a"]),
    (list[list[int]], [[1]]),
]

_UNSUPPORTED: list[tuple[typing.Any, object]] = [
    (Seat, {"section": "A", "number": 1}),
    (list[Seat], [{"section": "A", "number": 1}]),
    (tuple[Seat, ...], [{"section": "A", "number": 1}]),
    (int | str, 1),
    (tuple, [1]),
    (dict[str, int], {"a": 1}),
    (set[int], [1]),
    # Not classes at all, so they reach the dispatch's final fall-through
    # rather than any of its branches -- the case a bare `Any` annotation is.
    (typing.Any, 1),
    (typing.Callable[[int], int], 1),
]


@pytest.mark.parametrize(("annotation", "value"), _SUPPORTED)
def test_the_predicate_agrees_with_the_coercion_on_what_it_takes(
    annotation: typing.Any, value: object
) -> None:
    """A predicate that said yes where the coercion says no would let the
    startup check pass an event that dead-letters on delivery."""
    assert DataclassCodec().supported_annotation(annotation) is True

    _coerce(value, annotation, Seat, "field", 1)


@pytest.mark.parametrize(("annotation", "value"), _UNSUPPORTED)
def test_the_predicate_agrees_with_the_coercion_on_what_it_refuses(
    annotation: typing.Any, value: object
) -> None:
    """And one that said no where the coercion says yes would refuse a working
    project at startup, which is the more visible failure and still wrong."""
    assert DataclassCodec().supported_annotation(annotation) is False

    with pytest.raises(UnsupportedPayloadType):
        _coerce(value, annotation, Seat, "field", 1)


def test_dacite_widens_the_predicate_to_nesting() -> None:
    codec = DaciteCodec()

    assert codec.supported_annotation(Seat) is True
    assert codec.supported_annotation(tuple[Seat, ...]) is True
    assert codec.supported_annotation(list[Seat]) is True
    assert codec.supported_annotation(set[int]) is False


def test_a_nested_leaf_the_codec_cannot_rebuild_is_still_reported() -> None:
    """Naming the outer field, which is the one the declaration can change."""
    assert DaciteCodec().supported_annotation(WrapsASet) is False


# --------------------------------------------------------------------------
# The check that would have caught it
# --------------------------------------------------------------------------


def test_an_undecodable_event_is_refused_at_startup() -> None:
    """Previously: recorded fine, dead-lettered on delivery, `check` silent."""

    @event(name="tests.NestedUnderFlatCodec")
    @dataclass(frozen=True, slots=True)
    class NestedUnderFlatCodec:
        seats: tuple[Seat, ...]

    try:
        with override_settings(
            DJANGO_DOMAIN_EVENTS={
                "CODEC": "django_domain_events.codecs.dataclass_codec.DataclassCodec"
            }
        ):
            problems = checks.check_declared_events_are_decodable()
    finally:
        del registry._events_by_name["tests.NestedUnderFlatCodec"]
        del registry._events_by_class[NestedUnderFlatCodec]

    assert [p.id for p in problems] == ["django_domain_events.E005"]
    assert "seats" in problems[0].msg
    assert "dacite" in problems[0].hint


def test_the_same_event_passes_under_the_codec_that_can_read_it() -> None:
    @event(name="tests.NestedUnderDacite")
    @dataclass(frozen=True, slots=True)
    class NestedUnderDacite:
        seats: tuple[Seat, ...]

    try:
        with override_settings(
            DJANGO_DOMAIN_EVENTS={"CODEC": "django_domain_events.codecs.dacite_codec.DaciteCodec"}
        ):
            assert checks.check_declared_events_are_decodable() == []
    finally:
        del registry._events_by_name["tests.NestedUnderDacite"]
        del registry._events_by_class[NestedUnderDacite]


def test_a_codec_that_cannot_answer_is_not_interrogated() -> None:
    """This package should not guess at a codec it did not write."""

    with override_settings(DJANGO_DOMAIN_EVENTS={"CODEC": f"{__name__}.OpaqueCodec"}):
        assert checks.check_declared_events_are_decodable() == []


def test_unresolvable_annotations_are_left_to_the_decode_time_refusal() -> None:
    @dataclass(frozen=True, slots=True)
    class Local:
        later: NotDefinedAnywhere  # noqa: F821

    assert DataclassCodec().unsupported_fields(Local) == []


# --------------------------------------------------------------------------
# The settings block, whose failure mode is silence
# --------------------------------------------------------------------------


def test_a_misnamed_settings_block_is_reported() -> None:
    with override_settings(DOMAIN_EVENTS={"CODEC": "x"}):
        problems = checks.check_settings_keys_are_known()

    assert [p.id for p in problems] == ["django_domain_events.W006"]
    assert "DJANGO_DOMAIN_EVENTS" in problems[0].msg


def test_an_unknown_key_is_reported_with_the_valid_set() -> None:
    with override_settings(DJANGO_DOMAIN_EVENTS={"RELAY_BATCH_SIZE": 50}):
        problems = checks.check_settings_keys_are_known()

    assert [p.id for p in problems] == ["django_domain_events.W007"]
    assert "RELAY_BATCH_SIZE" in problems[0].msg
    assert "BATCH_SIZE" in problems[0].hint


def test_a_correct_settings_block_is_quiet() -> None:
    with override_settings(DJANGO_DOMAIN_EVENTS={"BATCH_SIZE": 10}):
        assert checks.check_settings_keys_are_known() == []


# --------------------------------------------------------------------------
# The label reaches the receiver that asked for the context
# --------------------------------------------------------------------------


@pytest.mark.django_db
def test_the_inline_context_carries_the_actor_label(order: OrderPlaced) -> None:
    """The fire-time construction site."""
    seen: list[str] = []

    with (
        _capturing_label(seen, mode="inline"),
        attributed(actor_key="auth.User:1", actor_label="dolores"),
        transaction.atomic(),
    ):
        fire(order)

    assert seen == ["dolores"]


@pytest.mark.django_db
def test_the_durable_context_carries_the_actor_label(order: OrderPlaced) -> None:
    """The relay construction site, which reads the row rather than the scope.

    Both sites matter and they are different code: a durable delivery can run
    hours later, in another process, with the firing scope long gone -- so the
    label has to come off the event row, and this is what proves it does.
    """
    seen: list[str] = []

    with _capturing_label(seen, mode="durable"):
        with (
            attributed(actor_key="auth.User:1", actor_label="dolores"),
            transaction.atomic(),
        ):
            fire(order)
        drain_outbox()

    assert seen == ["dolores"]


@contextmanager
def _capturing_label(seen: list[str], *, mode: str):
    """Swap one testapp receiver for one that records the label it was handed."""
    key = "testapp.inline_with_context" if mode == "inline" else "testapp.with_context"
    entry = registry.receiver_for_key(key)

    def record(evt: object, ctx: DeliveryContext) -> None:
        seen.append(ctx.actor_label)

    registry._receivers[key] = dataclasses.replace(entry, func=record)
    try:
        yield
    finally:
        registry._receivers[key] = entry
