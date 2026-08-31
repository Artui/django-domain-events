"""Tests mirroring ``django_domain_events/fire.py``."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from django.db import transaction

from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced, PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def test_an_unregistered_class_refuses(record: list[str]) -> None:
    """Silently accepting one would write a row nothing can ever decode."""

    @dataclass(frozen=True)
    class Undeclared:
        value: int

    with pytest.raises(LookupError, match="not registered"):
        fire(Undeclared(value=1))


def test_the_event_row_carries_the_registered_name_and_version(order: OrderPlaced) -> None:
    with transaction.atomic():
        fire(PinnedName(value=2))
    row = EventRecord.objects.get()
    assert (row.name, row.version) == ("testapp.pinned", 3)


def test_one_delivery_row_per_durable_receiver_and_none_for_the_others(
    order: OrderPlaced, record: list[str]
) -> None:
    """Two of the four receivers are durable. INLINE and ON_COMMIT get no row,
    which is the honest expression of what they promise: one cannot be owed
    because it rolls back, the other is explicitly losable."""
    with transaction.atomic():
        fire(order)

    keys = set(DeliveryRecord.objects.values_list("receiver_key", flat=True))
    assert keys == {"testapp.durable_receiver", "testapp.with_context"}
    assert DeliveryRecord.objects.filter(status=DeliveryStatus.PENDING).count() == 2


def test_inline_runs_before_fire_returns_and_on_commit_waits(
    order: OrderPlaced, record: list[str]
) -> None:
    """The distinction the two modes exist for, observed rather than asserted
    from the declaration."""
    with transaction.atomic():
        fire(order)
        # Both inline receivers have already run, and neither on_commit one has.
        assert record == ["inline:7", "inline_context:testapp.OrderPlaced:1"]
    assert record[-1] == "on_commit:7"


def test_an_inline_receiver_raising_takes_the_event_row_with_it(
    order: OrderPlaced, record: list[str]
) -> None:
    """The reason INLINE needs no durability: its failure mode is a rollback, so
    the business change and the event both revert and nothing is owed."""
    from django_domain_events.registry import registry

    receiver = registry.receiver_for_key("testapp.inline_receiver")
    original = receiver.func

    def explode(evt: OrderPlaced) -> None:
        raise RuntimeError("veto")

    object.__setattr__(receiver, "func", explode)
    try:
        with pytest.raises(RuntimeError, match="veto"), transaction.atomic():
            fire(order)
    finally:
        object.__setattr__(receiver, "func", original)

    assert EventRecord.objects.count() == 0
    assert DeliveryRecord.objects.count() == 0


def test_firing_outside_a_transaction_warns(order: OrderPlaced, record: list[str]) -> None:
    """In autocommit the event insert is its own transaction, so the dual-write
    gap is back and DURABLE is quietly no better than ON_COMMIT. Warn rather
    than pretend."""
    with pytest.warns(UserWarning, match="outside a transaction"):
        fire(order)


def test_the_warning_can_be_turned_off(order: OrderPlaced, record: list[str], settings) -> None:
    """A project that fires outside a transaction knowingly should not have to
    read the same warning forever."""
    settings.DJANGO_DOMAIN_EVENTS = {"WARN_OUTSIDE_ATOMIC": False}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        fire(order)
    assert [str(w.message) for w in caught] == []


def test_dedupe_key_and_occurred_at_are_recorded(order: OrderPlaced, record: list[str]) -> None:
    """occurred_at is domain time and differs from recorded_at on a backfill,
    which is exactly the case the two columns exist for."""
    happened = datetime(2020, 1, 1, tzinfo=timezone.utc)
    with transaction.atomic():
        fire(order, dedupe_key="order-7", occurred_at=happened)
    row = EventRecord.objects.get()
    assert row.dedupe_key == "order-7"
    assert row.occurred_at == happened
    assert row.recorded_at > happened


def test_max_attempts_is_copied_onto_the_row(order: OrderPlaced, record: list[str]) -> None:
    """Read off the row rather than the live declaration, so lowering the limit
    later cannot retroactively dead-letter work already in flight."""
    with transaction.atomic():
        fire(order)
    assert set(DeliveryRecord.objects.values_list("max_attempts", flat=True)) == {5}
