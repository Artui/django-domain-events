"""Shared fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from django_domain_events.registry import registry
from django_domain_events.types.registered_event import RegisteredEvent
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.testapp.events import Currency, OrderPlaced, calls


@pytest.fixture
def record() -> Iterator[list[str]]:
    """The list receivers append to, emptied around each test."""
    calls.clear()
    yield calls
    calls.clear()


@pytest.fixture
def order() -> OrderPlaced:
    """One fully populated event, so a round trip covers every scalar."""
    return OrderPlaced(
        order_id=7,
        total=Decimal("19.99"),
        placed_at=datetime(2026, 8, 31, 9, 0, tzinfo=timezone.utc),
        trace=UUID("d29eb6e4-a54c-4f06-8e3b-f1c416264d37"),
        currency=Currency.EUR,
        kind="retail",
        tags=["priority", "gift"],
    )


@contextmanager
def receiver_deleted(key: str) -> Iterator[None]:
    """Take a receiver out of the live registry, and put it back where it was.

    The whole dict is restored, not just the entry. Re-inserting a popped key
    appends it, and receivers are read in insertion order - so a pop-and-restore
    silently reorders the fan-out for every test that runs afterwards, which
    only shows up as a different delivery order in the full suite.
    """
    original = dict(registry._receivers)
    del registry._receivers[key]
    try:
        yield
    finally:
        registry._receivers.clear()
        registry._receivers.update(original)


@contextmanager
def receiver_replaced(key: str, func: object) -> Iterator[None]:
    """Swap one receiver's callable for the duration of a test."""
    entry = registry.receiver_for_key(key)
    original = entry.func
    object.__setattr__(entry, "func", func)
    try:
        yield
    finally:
        object.__setattr__(entry, "func", original)


@contextmanager
def event_deleted(name: str) -> Iterator[None]:
    """Take one event out of the live registry, and put it back.

    Both indexes, because the registry keeps two and a check reading one while
    a test cleared the other would pass on a registry no consumer can produce.
    """
    entry = registry.event_for_name(name)
    assert entry is not None, f"{name} is not registered"
    del registry._events_by_name[name]
    del registry._events_by_class[entry.event_class]
    try:
        yield
    finally:
        registry.register_event(entry)


@contextmanager
def event_registered(event_class: type, name: str, version: int = 1) -> Iterator[None]:
    """Add an ad-hoc event for the duration of a test."""
    registry.register_event(RegisteredEvent(event_class=event_class, name=name, version=version))
    try:
        yield
    finally:
        registry._events_by_name.pop(name, None)
        registry._events_by_class.pop(event_class, None)


@contextmanager
def receiver_registered(entry: RegisteredReceiver) -> Iterator[None]:
    """Add an ad-hoc receiver for the duration of a test."""
    registry.register_receiver(entry)
    try:
        yield
    finally:
        registry._receivers.pop(entry.key, None)
