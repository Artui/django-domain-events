"""Tests mirroring ``django_domain_events/listens_for.py``."""

from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.listens_for import listens_for
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered


def test_it_names_the_event_a_receiver_is_declared_for() -> None:
    """The question a dead-letter row raises: it names a receiver key, and the
    next thing anyone wants is what it was supposed to be receiving."""
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
    """Two cases look identical from a delivery row - a key never declared, and
    one whose event class went away - and both leave the operator with nothing
    to decode."""
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
