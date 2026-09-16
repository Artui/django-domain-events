"""Tests mirroring ``django_domain_events/what_listens_to.py``."""

from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.introspection.what_listens_to import what_listens_to
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_deleted, receiver_registered
from tests.testapp.events import Eagerly, OrderPlaced, Unheard


def test_it_returns_every_receiver_sorted_by_key() -> None:
    keys = [r.key for r in what_listens_to(OrderPlaced)]
    assert keys == sorted(keys)
    assert "testapp.durable_receiver" in keys
    assert "testapp.with_context" in keys


def test_it_spans_every_mode() -> None:
    """Not only the durable ones: "who reacts to this" is a question about the
    code, and an inline receiver is as much a reaction as a queued one."""
    modes = {r.mode.value for r in what_listens_to(OrderPlaced)}
    assert modes == {"durable", "inline", "on_commit"}


def test_an_event_nobody_listens_to_returns_nothing() -> None:
    assert what_listens_to(Unheard) == []


def test_an_unregistered_class_returns_nothing_rather_than_raising() -> None:
    @dataclass(frozen=True)
    class Stranger:
        value: int

    assert what_listens_to(Stranger) == []


def test_deleting_a_receiver_removes_it_from_the_answer() -> None:
    with receiver_deleted("testapp.eager"):
        assert [r.key for r in what_listens_to(Eagerly)] == ["testapp.not_eager"]


WILDCARD = RegisteredReceiver(
    key="testapp.everything",
    event_class=AnyEvent,
    func=lambda evt: None,
    mode=DeliveryMode.DURABLE,
    takes_context=False,
    max_attempts=5,
    eager=False,
    site="relay",
)


def test_a_wildcard_is_the_plus_and_is_not_listed_per_event() -> None:
    """It receives the event, as it receives every event; listing a transport
    under each one says nothing about any of them."""
    with receiver_registered(WILDCARD):
        assert "testapp.everything" not in [r.key for r in what_listens_to(OrderPlaced)]
        assert what_listens_to(Unheard) == []


def test_asking_about_any_event_returns_exactly_the_wildcards() -> None:
    with receiver_registered(WILDCARD):
        assert what_listens_to(AnyEvent) == [WILDCARD]
    assert what_listens_to(AnyEvent) == []
