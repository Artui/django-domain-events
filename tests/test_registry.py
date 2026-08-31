"""Tests mirroring ``django_domain_events/registry.py``."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from django_domain_events.registry import Registry
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_event import RegisteredEvent
from django_domain_events.types.registered_receiver import RegisteredReceiver


@dataclass(frozen=True)
class Alpha:
    value: int


@dataclass(frozen=True)
class Beta:
    value: int


def _entry(cls: type, name: str) -> RegisteredEvent:
    return RegisteredEvent(event_class=cls, name=name, version=1)


def _receiver(key: str, cls: type, func) -> RegisteredReceiver:
    return RegisteredReceiver(
        key=key,
        event_class=cls,
        func=func,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
    )


def test_lookup_by_class_and_by_name() -> None:
    """Both directions are indexed because both are hot: firing looks up by
    class, and the relay looks up by the name written on the row."""
    r = Registry()
    r.register_event(_entry(Alpha, "app.Alpha"))

    assert r.event_for_class(Alpha).name == "app.Alpha"
    assert r.event_for_name("app.Alpha").event_class is Alpha
    assert r.event_for_class(Beta) is None
    assert r.event_for_name("app.Missing") is None


def test_re_registering_the_same_class_is_allowed() -> None:
    """Module reimport under a test runner must not look like a name clash."""
    r = Registry()
    r.register_event(_entry(Alpha, "app.Alpha"))
    r.register_event(_entry(Alpha, "app.Alpha"))
    assert len(r.events()) == 1


def test_two_classes_cannot_share_an_event_name() -> None:
    """Rows of one would decode as the other, which is silent data corruption
    rather than a mix-up someone notices."""
    r = Registry()
    r.register_event(_entry(Alpha, "app.Shared"))
    with pytest.raises(ValueError, match="already registered"):
        r.register_event(_entry(Beta, "app.Shared"))


def test_two_receivers_cannot_share_a_key() -> None:
    """Delivery rows address receivers by key, so it has to name exactly one."""

    def one(evt: Alpha) -> None: ...
    def two(evt: Alpha) -> None: ...

    r = Registry()
    r.register_receiver(_receiver("app.same", Alpha, one))
    with pytest.raises(ValueError, match="already registered"):
        r.register_receiver(_receiver("app.same", Alpha, two))


def test_re_registering_the_same_receiver_is_allowed() -> None:
    def one(evt: Alpha) -> None: ...

    r = Registry()
    r.register_receiver(_receiver("app.same", Alpha, one))
    r.register_receiver(_receiver("app.same", Alpha, one))
    assert len(r.receivers()) == 1


def test_receivers_are_selected_by_event_class() -> None:
    def a(evt: Alpha) -> None: ...
    def b(evt: Beta) -> None: ...

    r = Registry()
    r.register_receiver(_receiver("app.a", Alpha, a))
    r.register_receiver(_receiver("app.b", Beta, b))

    assert [x.key for x in r.receivers_for(Alpha)] == ["app.a"]
    assert r.receiver_for_key("app.b").func is b
    assert r.receiver_for_key("app.gone") is None


def test_clear_forgets_everything() -> None:
    r = Registry()
    r.register_event(_entry(Alpha, "app.Alpha"))
    r.register_receiver(_receiver("app.a", Alpha, lambda evt: None))
    r.clear()
    assert r.events() == []
    assert r.receivers() == []
    assert r.event_for_name("app.Alpha") is None
