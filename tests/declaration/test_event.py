"""Tests mirroring ``django_domain_events/event.py``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pytest

from django_domain_events.declaration.event import event
from django_domain_events.declaration.registry import registry
from django_domain_events.types.registered_event import RegisteredEvent
from tests.testapp.events import OrderPlaced, PinnedName


def test_the_bare_form_registers_under_the_app_label() -> None:
    assert registry.event_for_class(OrderPlaced).name == "testapp.OrderPlaced"


def test_the_called_form_takes_a_name_and_a_version() -> None:
    """Pinning the name is what lets a class be renamed without stranding the
    rows already written under the old one."""
    entry = registry.event_for_class(PinnedName)
    assert (entry.name, entry.version) == ("testapp.pinned", 3)


def test_the_decorator_returns_the_class_unchanged() -> None:
    """Decorating must not wrap: consumers construct these directly, and a
    wrapper would break isinstance and the constructor signature alike."""

    @dataclass(frozen=True)
    class Local:
        value: int

    returned = event(name="testapp.returned")(Local)
    assert returned is Local
    registry._events_by_class.pop(Local, None)
    registry._events_by_name.pop("testapp.returned", None)


def test_a_mutable_dataclass_is_refused_at_declaration() -> None:
    @dataclass
    class Mutable:
        value: int

    with pytest.raises(TypeError, match="mutable dataclass"):
        event(name="testapp.mutable")(Mutable)


def _declared(**options: object) -> RegisteredEvent:
    """Declare a throwaway event with these options, and return its entry."""

    @dataclass(frozen=True)
    class Local:
        value: int

    event(name="testapp.retained", **options)(Local)
    entry = registry.event_for_class(Local)
    registry._events_by_class.pop(Local, None)
    registry._events_by_name.pop("testapp.retained", None)
    assert entry is not None
    return entry


def test_retention_defaults_to_the_ordinary_window() -> None:
    assert _declared().retention is None


def test_a_window_of_its_own_is_kept_on_the_registration() -> None:
    assert _declared(retention=timedelta(days=7)).retention == timedelta(days=7)


@pytest.mark.parametrize("retention", [3600, "succeeded", True, 7.0])
def test_a_retention_that_is_neither_a_window_nor_a_policy_is_refused(retention: object) -> None:
    """A bare number means seconds to one reader and days to the next, so it is
    refused rather than guessed at."""
    with pytest.raises(TypeError, match="retention="):
        _declared(retention=retention)


@pytest.mark.parametrize(
    "retention",
    [
        timedelta(0),
        timedelta(seconds=-1),
        timedelta(milliseconds=500),
        timedelta(milliseconds=1500),
        timedelta(seconds=2**31),
    ],
    ids=["zero", "negative", "half-a-second", "a-second-and-a-half", "past-the-column"],
)
def test_a_window_the_column_cannot_hold_is_refused(retention: timedelta) -> None:
    """Half a second would be stored as zero, which deletes at once; past 2**31
    seconds passes on SQLite and fails every fire() on Postgres."""
    with pytest.raises(ValueError, match="retention="):
        _declared(retention=retention)


@pytest.mark.parametrize("retention", [timedelta(seconds=1), timedelta(seconds=2**31 - 1)])
def test_the_bounds_themselves_are_accepted(retention: timedelta) -> None:
    assert _declared(retention=retention).retention == retention
