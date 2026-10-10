"""Tests mirroring ``django_domain_events/registry.py``."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

import pytest

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.registry import Registry
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
        site="relay",
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


def test_a_wildcard_is_returned_for_every_class_in_declaration_order() -> None:
    def a(evt: Alpha) -> None: ...
    def everything(evt: object) -> None: ...
    def b(evt: Beta) -> None: ...

    r = Registry()
    r.register_receiver(_receiver("app.a", Alpha, a))
    r.register_receiver(_receiver("app.everything", AnyEvent, everything))
    r.register_receiver(_receiver("app.b", Beta, b))

    assert [x.key for x in r.receivers_for(Alpha)] == ["app.a", "app.everything"]
    assert [x.key for x in r.receivers_for(Beta)] == ["app.everything", "app.b"]


def test_a_wildcard_is_matched_when_asked_not_when_declared() -> None:
    """A class the registry has never heard of still gets the wildcard: nothing
    is expanded at declaration, so nothing can be missed by declaring early."""

    @dataclass(frozen=True)
    class DeclaredLater:
        value: int

    r = Registry()
    r.register_receiver(_receiver("app.everything", AnyEvent, lambda evt: None))
    r.register_event(_entry(DeclaredLater, "app.DeclaredLater"))

    assert [x.key for x in r.receivers_for(DeclaredLater)] == ["app.everything"]


def _laned(r: Registry) -> Registry:
    """Two receivers in ``mail``, one in ``search``, one in the default lane."""
    for key, lane in [("app.a", "mail"), ("app.b", "search"), ("app.c", "default")]:
        r.register_receiver(dataclasses.replace(_receiver(key, Alpha, print), lane=lane))
    r.register_receiver(dataclasses.replace(_receiver("app.d", Beta, print), lane="mail"))
    return r


def test_a_named_lane_is_the_receivers_declared_in_it() -> None:
    r = _laned(Registry())

    assert r.receiver_keys_in_lane("mail") == ["app.a", "app.d"]
    assert r.receiver_keys_in_lane("search") == ["app.b"]
    assert r.receiver_keys_in_lane("nobody") == []


def test_the_default_lane_is_defined_by_what_the_named_lanes_take() -> None:
    """Every key in a named lane, so the default lane can be phrased as
    everything else - which is what reaches a row whose receiver was deleted."""
    r = _laned(Registry())

    assert r.receiver_keys_in_named_lanes() == ["app.a", "app.b", "app.d"]
    assert Registry().receiver_keys_in_named_lanes() == []


def test_the_declared_lanes_are_listed() -> None:
    assert _laned(Registry()).lanes() == ["default", "mail", "search"]
    assert Registry().lanes() == ["default"]


def test_a_lane_nobody_declared_is_refused() -> None:
    """A relay started for a misspelt lane would claim nothing, forever, and
    look healthy doing it."""
    r = _laned(Registry())

    with pytest.raises(ValueError, match=r"No receiver is declared in lane 'mial'.*mail, search"):
        r.require_lane("mial")


@pytest.mark.parametrize("lane", ["mail", "default", None])
def test_a_declared_lane_the_default_and_every_lane_are_accepted(lane: str | None) -> None:
    """The default lane exists with no receiver in it - it is where a deleted
    receiver's rows drain - and None is every lane at once."""
    _laned(Registry()).require_lane(lane)
    Registry().require_lane("default")
