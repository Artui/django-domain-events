"""Tests mirroring ``django_domain_events/utils.py``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.utils import (
    label_for,
    require_frozen_dataclass,
    resolve_targets,
)


def test_label_comes_from_the_app_label_not_the_import_path() -> None:
    """The path is a refactor away from orphaning every row that names it."""
    assert label_for("tests.testapp.events", "OrderPlaced") == "testapp.OrderPlaced"


def test_a_module_outside_any_installed_app_refuses() -> None:
    """A made-up label would be written onto rows and only surface later, as a
    mismatch nobody can trace back to this moment."""
    with pytest.raises(LookupError, match="not inside an installed app"):
        label_for("some.unrelated.module", "Thing")


def test_a_non_dataclass_is_rejected() -> None:
    class NotADataclass:
        pass

    with pytest.raises(TypeError, match="is not a dataclass"):
        require_frozen_dataclass(NotADataclass)


def test_a_mutable_dataclass_is_rejected() -> None:
    """At-least-once delivery hands a different instance to every attempt, so a
    receiver mutating one is writing to a copy it is about to discard."""

    @dataclass
    class Mutable:
        value: int

    with pytest.raises(TypeError, match="mutable dataclass"):
        require_frozen_dataclass(Mutable)


def test_a_frozen_dataclass_passes() -> None:
    @dataclass(frozen=True)
    class Frozen:
        value: int

    assert require_frozen_dataclass(Frozen) is None


def test_a_datetime_parses_from_exactly_what_the_encoder_writes() -> None:
    """Pinned against the encoder's real output rather than a hand-typed string.

    A round trip proves the two halves agree; it does not prove either is right.
    This asserts the shape actually written, because the bug it guards was a
    disagreement between them that only appeared on Python 3.10 to 3.12, where
    ``datetime.fromisoformat`` cannot read the trailing Z the encoder emits.
    """
    import json
    from datetime import datetime, timezone

    from django.core.serializers.json import DjangoJSONEncoder

    from django_domain_events.utils import parse_datetime

    value = datetime(2026, 8, 31, 9, tzinfo=timezone.utc)
    written = json.loads(json.dumps(value, cls=DjangoJSONEncoder))

    assert written.endswith("Z")
    assert parse_datetime(written) == value


def test_an_offset_datetime_parses_too() -> None:
    from datetime import datetime, timedelta, timezone

    from django_domain_events.utils import parse_datetime

    assert parse_datetime("2026-08-31T09:00:00+02:00") == datetime(
        2026, 8, 31, 9, tzinfo=timezone(timedelta(hours=2))
    )


def _context() -> DeliveryContext:
    return DeliveryContext(
        event_id=1, event_name="testapp.Unheard", attempt=1, actor_key="", actor_label="", scope={}
    )


def _resolve(*returned: object) -> list[str]:
    return resolve_targets("probe.fan", lambda event, context: list(returned), object(), _context())


def test_targets_come_back_in_the_order_the_callable_gave_them() -> None:
    assert _resolve("c", "a", "b") == ["c", "a", "b"]


def test_a_target_returned_twice_is_delivered_once() -> None:
    """One delivery per target is the promise, and the unique constraint would
    otherwise turn a callable that reached one destination twice into a failed
    transaction."""
    assert _resolve("a", "b", "a") == ["a", "b"]


def test_any_iterable_will_do() -> None:
    def generated(event: object, context: DeliveryContext) -> Iterator[str]:
        yield from ("x", "y")

    assert resolve_targets("probe.fan", generated, object(), _context()) == ["x", "y"]


def test_a_target_that_is_not_a_string_is_refused_by_receiver() -> None:
    """The column would store ``str(42)`` while a replay compared ``42``, and the
    two would never match."""
    with pytest.raises(TypeError, match=r"targets for receiver 'probe.fan' returned 42, a int"):
        _resolve("a", 42)


def test_a_blank_target_is_refused() -> None:
    """Blank is what a receiver without targets= writes."""
    with pytest.raises(ValueError, match="returned an empty string"):
        _resolve("a", "")
