"""Shared fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from django_domain_events.registry import registry
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
