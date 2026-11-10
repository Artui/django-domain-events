from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.listens_for import listens_for
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered


def test_it_names_the_event_a_receiver_is_declared_for() -> None:
    """A dead-letter row names a receiver key, and the operator wants its event."""
    entry = listens_for("testapp.durable_receiver")
    assert entry is not None
    assert entry.name == "testapp.OrderPlaced"


def test_it_reports_the_pinned_name_not_the_class_name() -> None:
    entry = listens_for("testapp.eager")
    assert entry is not None
    assert entry.name == "testapp.Eagerly"


def test_an_unknown_key_is_none() -> None:
    assert listens_for("nothing.at.all") is None


@dataclass(frozen=True)
class NeverDeclared:
    value: int


def test_a_receiver_whose_event_was_deleted_is_also_none() -> None:
    """Two cases look identical from a delivery row, and both leave nothing to decode."""
    entry = RegisteredReceiver(
        key="tests.dangling",
        event_class=NeverDeclared,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
    )
    with receiver_registered(entry):
        assert listens_for("tests.dangling") is None


def test_a_wildcard_names_no_single_event() -> None:
    """Owed every event, so there is no one answer; the delivery row names its event."""
    wildcard = RegisteredReceiver(
        key="testapp.everything",
        event_class=AnyEvent,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
    )
    with receiver_registered(wildcard):
        assert listens_for("testapp.everything") is None
