"""Tests for the ``upgrade`` hook, across ``utils.decode_payload``.

The hook is the escape from the two payload changes the schema rule calls
breaking: a field added without a default, and a type changed incompatibly.
Without it, a row written before that change dead-letters with no forward path
except editing JSON by hand.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from django.db import transaction

from django_domain_events.assert_fired import assert_fired
from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.event import event
from django_domain_events.fire import fire
from django_domain_events.models.event_record import EventRecord
from django_domain_events.payload_upgrade_failed import PayloadUpgradeFailed
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import decode_payload
from tests.conftest import event_registered, receiver_registered


@dataclass(frozen=True)
class Migrated:
    """v2 added a required field, which the schema rule calls breaking."""

    order_id: int
    currency: str

    @staticmethod
    def upgrade(payload: dict[str, Any], from_version: int) -> dict[str, Any]:
        return {**payload, "currency": "EUR"}


def test_an_older_row_is_migrated_before_it_is_decoded() -> None:
    """A required field added in v2 makes every v1 row undecodable. This is the
    only forward path that does not involve editing JSON by hand."""
    with event_registered(Migrated, "tests.migrated", version=2):
        rebuilt = decode_payload(Migrated, {"order_id": 7}, 1)
    assert rebuilt == Migrated(order_id=7, currency="EUR")


def test_a_current_row_skips_the_hook() -> None:
    called = []

    @dataclass(frozen=True)
    class Counting:
        value: int

        @staticmethod
        def upgrade(payload: dict[str, Any], from_version: int) -> dict[str, Any]:
            called.append(from_version)
            return payload

    with event_registered(Counting, "tests.counting", version=2):
        assert decode_payload(Counting, {"value": 1}, 2) == Counting(value=1)
    assert called == []


def test_a_row_from_the_future_skips_it_too() -> None:
    """A rollback, and no forward migration helps: the code that would know how
    to read that row is the code that was just removed."""
    called = []

    @dataclass(frozen=True)
    class Rolled:
        value: int

        @staticmethod
        def upgrade(payload: dict[str, Any], from_version: int) -> dict[str, Any]:
            called.append(from_version)
            return payload

    with event_registered(Rolled, "tests.rolled", version=1):
        decode_payload(Rolled, {"value": 1}, 5)
    assert called == []


def test_the_hook_is_told_which_version_it_is_reading() -> None:
    seen = []

    @dataclass(frozen=True)
    class Versioned:
        value: int

        @staticmethod
        def upgrade(payload: dict[str, Any], from_version: int) -> dict[str, Any]:
            seen.append(from_version)
            return payload

    with event_registered(Versioned, "tests.versioned", version=4):
        decode_payload(Versioned, {"value": 1}, 2)
    assert seen == [2]


def test_a_failing_hook_says_so_by_name() -> None:
    """It lands in last_error, and an operator reading it needs to know the
    hook ran rather than that something went wrong between row and receiver."""

    @dataclass(frozen=True)
    class Broken:
        value: int

        @staticmethod
        def upgrade(payload: dict[str, Any], from_version: int) -> dict[str, Any]:
            raise KeyError("legacy_field")

    with (
        event_registered(Broken, "tests.broken", version=2),
        pytest.raises(PayloadUpgradeFailed, match=r"Broken.upgrade\(\) failed"),
    ):
        decode_payload(Broken, {"value": 1}, 1)


def test_an_ordinary_method_is_refused_at_the_decorator() -> None:
    """Reached through the class it is unbound, so the payload would silently
    arrive as ``self`` - and it would arrive there in the relay, hours later."""
    with pytest.raises(TypeError, match="staticmethod or a classmethod"):

        @event(name="tests.bad_hook")
        @dataclass(frozen=True)
        class BadHook:
            value: int

            def upgrade(self, payload, from_version):  # never called: the decorator refuses it
                return payload


def test_a_hook_with_the_wrong_arity_is_refused_at_the_decorator() -> None:
    with pytest.raises(TypeError, match=r"accept \(payload, from_version\)"):

        @event(name="tests.bad_arity")
        @dataclass(frozen=True)
        class BadArity:
            value: int

            @staticmethod
            def upgrade(payload):  # never called: the decorator refuses it
                return payload


def test_a_classmethod_hook_is_accepted() -> None:
    @dataclass(frozen=True)
    class ByClass:
        value: int
        extra: str

        @classmethod
        def upgrade(cls, payload: dict[str, Any], from_version: int) -> dict[str, Any]:
            return {**payload, "extra": cls.__name__}

    with event_registered(ByClass, "tests.by_class", version=2):
        assert decode_payload(ByClass, {"value": 1}, 1) == ByClass(value=1, extra="ByClass")


def test_an_event_without_a_hook_is_untouched() -> None:
    @dataclass(frozen=True)
    class Plain:
        value: int

    with event_registered(Plain, "tests.plain", version=3):
        assert decode_payload(Plain, {"value": 1}, 1) == Plain(value=1)


def test_an_unregistered_class_decodes_without_a_version_to_compare() -> None:
    """decode_payload is reachable for a class the registry does not know, and
    there is then no declared version to say whether the row is old."""
    assert decode_payload(Migrated, {"order_id": 1, "currency": "USD"}, 1) == Migrated(
        order_id=1, currency="USD"
    )


@event(name="tests.evolved", version=2)
@dataclass(frozen=True)
class Evolved:
    order_id: int
    currency: str

    @staticmethod
    def upgrade(payload: dict[str, Any], from_version: int) -> dict[str, Any]:
        return {**payload, "currency": "EUR"}


@pytest.mark.django_db
def test_the_relay_delivers_a_row_written_before_the_field_existed(record: list[str]) -> None:
    """End to end: without the hook this dead-letters, because the codec cannot
    build a required field the row never carried."""
    seen = []

    entry = RegisteredReceiver(
        key="tests.evolved_receiver",
        event_class=Evolved,
        func=seen.append,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
    )
    with receiver_registered(entry):
        with transaction.atomic():
            fire(Evolved(order_id=7, currency="USD"))
        # Rewrite the row as the previous deploy would have left it.
        EventRecord.objects.update(version=1, payload={"order_id": 7})

        counts = drain_outbox()

    assert counts == {DeliveryStatus.SUCCEEDED: 1}
    assert seen == [Evolved(order_id=7, currency="EUR")]


@pytest.mark.django_db
def test_assert_fired_reads_the_same_row_the_relay_would(record: list[str]) -> None:
    """A test helper that decodes a payload the relay would reject is how a
    suite comes to agree with a bug."""
    with transaction.atomic():
        fire(Evolved(order_id=7, currency="USD"))
    EventRecord.objects.update(version=1, payload={"order_id": 7})

    assert assert_fired(Evolved, times=1) == [Evolved(order_id=7, currency="EUR")]
