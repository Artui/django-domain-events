"""Tests mirroring ``django_domain_events/assert_fired.py``."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from django.db import transaction

from django_domain_events.assert_fired import assert_fired
from django_domain_events.fire import fire
from tests.testapp.events import OrderPlaced, PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_reads_the_log_rather_than_a_mock(order: OrderPlaced, record: list[str]) -> None:
    """A mock records that a function was called; the row is what the rest of the
    system reacts to. If the payload could not be encoded, a mock still passes."""
    with transaction.atomic():
        fire(order)
    rows = assert_fired(OrderPlaced)
    assert len(rows) == 1
    assert rows[0].payload["order_id"] == 7


def test_an_exact_count_can_be_required(record: list[str]) -> None:
    with transaction.atomic():
        fire(PinnedName(value=1))
        fire(PinnedName(value=2))
    assert len(assert_fired(PinnedName, times=2)) == 2


def test_the_wrong_count_fails(record: list[str]) -> None:
    with transaction.atomic():
        fire(PinnedName(value=1))
    with pytest.raises(AssertionError, match=r"2 time\(s\), found 1"):
        assert_fired(PinnedName, times=2)


def test_never_fired_fails(record: list[str]) -> None:
    with pytest.raises(AssertionError, match="but it was not"):
        assert_fired(PinnedName)


def test_an_unregistered_class_refuses() -> None:
    @dataclass(frozen=True)
    class Undeclared:
        value: int

    with pytest.raises(LookupError, match="not registered"):
        assert_fired(Undeclared)
