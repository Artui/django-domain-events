"""Shared fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

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
