"""Tests mirroring ``django_domain_events/fire.py``."""

from __future__ import annotations

import importlib
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest import mock

import pytest
from django.contrib.auth.models import User
from django.db import transaction

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.scope.attributed import attributed
from django_domain_events.scope.suppressed import suppressed
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import Eagerly, OrderPlaced, PinnedName, Unheard

pytestmark = pytest.mark.django_db(transaction=True)

# By module object rather than dotted path: the package root re-exports the
# function under the module's own name, so the dotted path walks into it.
_fire_module = importlib.import_module("django_domain_events.delivery.fire")


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    """Take this file's ad-hoc receivers back out of the process-wide registry."""
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


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
    from django_domain_events.declaration.registry import registry

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


def test_an_eager_receiver_delivers_at_commit_without_a_relay(record: list[str]) -> None:
    """What stops DURABLE feeling slow: outbox durability at on-commit latency.

    The row is still written in the transaction, so what this buys is only the
    attempt - anything a crash loses is still owed and the relay reclaims it.
    """
    with transaction.atomic():
        fire(Eagerly(value=3))
    assert "eager:3" in record

    row = DeliveryRecord.objects.get(receiver_key="testapp.eager")
    assert row.status == DeliveryStatus.SUCCEEDED


def test_a_non_eager_receiver_waits_for_the_relay(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    assert "durable:7" not in record
    assert (
        DeliveryRecord.objects.get(receiver_key="testapp.durable_receiver").status
        == DeliveryStatus.PENDING
    )


def test_an_eager_receiver_raising_leaves_the_row_owed(record: list[str]) -> None:
    """The relay is the fallback, so an eager failure is a retry rather than a
    loss. robust=True also keeps it from cancelling the other callbacks."""
    from django_domain_events.declaration.registry import registry

    entry = registry.receiver_for_key("testapp.eager")
    original = entry.func

    def explode(evt: OrderPlaced) -> None:
        raise RuntimeError("not yet")

    object.__setattr__(entry, "func", explode)
    try:
        with transaction.atomic():
            fire(Eagerly(value=3))
    finally:
        object.__setattr__(entry, "func", original)

    row = DeliveryRecord.objects.get(receiver_key="testapp.eager")
    assert row.status == DeliveryStatus.FAILED
    assert row.attempts == 1
    assert row.available_at > row.event.recorded_at


def _rows(key: str) -> list[tuple[str, str, int]]:
    return list(
        DeliveryRecord.objects.filter(receiver_key=key)
        .order_by("target")
        .values_list("target", "status", "attempts")
    )


def test_a_fan_out_writes_one_row_per_target_each_with_its_own_attempts() -> None:
    """Three targets, three rows, and a failing one does not drag the others.

    One claim of all three, so each is attempted exactly once, then the relay
    left to spend the failing target's whole budget: the two that succeeded
    must still read one attempt each at the end.
    """
    delivered: list[str] = []
    failures: list[DeliveryFailure] = []

    def deliver(event: Unheard, context: DeliveryContext) -> None:
        delivered.append(context.target)
        if context.target == "b":
            raise RuntimeError("b is down")

    receiver(
        Unheard,
        key="probe.fan",
        takes_context=True,
        targets=lambda event, context: ["a", "b", "c"],
        on_failure=failures.append,
    )(deliver)
    with transaction.atomic():
        fire(Unheard(value=1))

    assert _rows("probe.fan") == [
        ("a", DeliveryStatus.PENDING, 0),
        ("b", DeliveryStatus.PENDING, 0),
        ("c", DeliveryStatus.PENDING, 0),
    ]

    deliver_pending(limit=3)
    assert sorted(delivered) == ["a", "b", "c"]
    assert _rows("probe.fan") == [
        ("a", DeliveryStatus.SUCCEEDED, 1),
        ("b", DeliveryStatus.FAILED, 1),
        ("c", DeliveryStatus.SUCCEEDED, 1),
    ]

    drain_outbox()
    assert _rows("probe.fan") == [
        ("a", DeliveryStatus.SUCCEEDED, 1),
        ("b", DeliveryStatus.DEAD, 5),
        ("c", DeliveryStatus.SUCCEEDED, 1),
    ]
    # The failure hook can say which target failed, not only which receiver.
    assert {failure.target for failure in failures} == {"b"}


def test_no_targets_writes_no_rows_and_raises_nothing() -> None:
    """Returning nothing is how a fan-out says "not this event"."""
    receiver(Unheard, key="probe.nobody", targets=lambda event, context: [])(lambda event: None)

    with mock.patch.object(_fire_module, "notify_relay") as notify, transaction.atomic():
        event_id = fire(Unheard(value=1))

    assert EventRecord.objects.filter(pk=event_id).exists()
    assert DeliveryRecord.objects.filter(event_id=event_id).count() == 0
    # Nothing owed, so nothing to wake a relay for.
    notify.assert_not_called()


def test_one_target_is_enough_to_wake_the_relay() -> None:
    """The control for the test above: the patch reaches the call, so its
    silence there is a finding rather than a patch that missed."""
    receiver(Unheard, key="probe.one", targets=lambda event, context: ["x"])(lambda event: None)

    with mock.patch.object(_fire_module, "notify_relay") as notify, transaction.atomic():
        fire(Unheard(value=1))

    notify.assert_called_once_with()


def test_the_callable_is_handed_the_event_and_the_fire_time_context() -> None:
    """Everything a data-driven lookup needs: the event, its name, and the scope
    it was fired under."""
    seen: list[tuple[object, DeliveryContext]] = []

    def targets(event: Unheard, context: DeliveryContext) -> list[str]:
        seen.append((event, context))
        return ["only"]

    receiver(Unheard, key="probe.lookup", targets=targets)(lambda event: None)
    with transaction.atomic(), attributed(actor_key="auth.User:9", tenant="acme"):
        event_id = fire(Unheard(value=4))

    [(event, context)] = seen
    assert event == Unheard(value=4)
    assert (context.event_id, context.event_name, context.attempt) == (
        event_id,
        "testapp.Unheard",
        1,
    )
    assert (context.actor_key, context.scope, context.target) == (
        "auth.User:9",
        {"tenant": "acme"},
        "",
    )


def test_a_target_is_written_as_long_as_it_was_returned() -> None:
    """No length is imposed on a target, in Python or by the column.

    Backend-dependent in half. Nothing in Python may refuse it on either
    backend. The column type is held only on Postgres, which refuses a value
    longer than a varchar's length where SQLite stores it anyway - so on SQLite
    this test passes with a bounded column, and on Postgres it does not.
    """
    long_target = "endpoint:" + "x" * 991
    receiver(Unheard, key="probe.long", targets=lambda event, context: [long_target])(
        lambda event: None
    )
    with transaction.atomic():
        fire(Unheard(value=1))

    assert _rows("probe.long") == [(long_target, DeliveryStatus.PENDING, 0)]
    assert len(long_target) == 1000


def test_a_raising_callable_fails_the_callers_transaction() -> None:
    """The business change, the event and the deliveries roll back together.

    A fan-out that swallowed this and delivered to nobody would leave a
    committed change whose consequences silently never happened.
    """

    def broken(event: Unheard, context: DeliveryContext) -> list[str]:
        raise RuntimeError("the endpoint table is locked")

    receiver(Unheard, key="probe.broken", targets=broken)(lambda event: None)

    with pytest.raises(RuntimeError, match="the endpoint table is locked"), transaction.atomic():
        User.objects.create(username="placed-an-order")
        fire(Unheard(value=1))

    assert not User.objects.filter(username="placed-an-order").exists()
    assert EventRecord.objects.count() == 0
    assert DeliveryRecord.objects.count() == 0


def test_a_plain_receiver_beside_a_fan_out_keeps_its_single_blank_row() -> None:
    receiver(Unheard, key="probe.fan", targets=lambda event, context: ["x", "y"])(lambda e: None)
    receiver(Unheard, key="probe.plain")(lambda event: None)

    with transaction.atomic():
        fire(Unheard(value=1))

    assert [target for target, _, _ in _rows("probe.plain")] == [""]
    assert [target for target, _, _ in _rows("probe.fan")] == ["x", "y"]


def test_an_eager_fan_out_attempts_every_target_at_commit() -> None:
    delivered: list[str] = []

    def deliver(event: Unheard, context: DeliveryContext) -> None:
        delivered.append(context.target)

    receiver(
        Unheard,
        key="probe.eager_fan",
        takes_context=True,
        eager=True,
        targets=lambda event, context: ["x", "y"],
    )(deliver)
    with transaction.atomic():
        fire(Unheard(value=1))

    assert sorted(delivered) == ["x", "y"]
    assert {status for _, status, _ in _rows("probe.eager_fan")} == {DeliveryStatus.SUCCEEDED}


def test_the_callable_is_not_called_for_a_suppressed_event() -> None:
    """Suppression is about the event: nothing is owed, so nothing is looked up."""
    targets = mock.Mock(return_value=["x"])
    receiver(Unheard, key="probe.suppressed", targets=targets)(lambda event: None)
    with transaction.atomic(), suppressed(Unheard, reason="backfill"):
        fire(Unheard(value=1))

    targets.assert_not_called()
