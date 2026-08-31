"""Tests mirroring ``django_domain_events/event.py``."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from django_domain_events.event import event
from django_domain_events.registry import registry
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
