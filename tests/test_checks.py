"""Tests mirroring ``django_domain_events/checks.py``."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

import pytest
from django.db import transaction

from django_domain_events import checks
from django_domain_events.fire import fire
from django_domain_events.registry import registry
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.testapp.events import OrderPlaced


@dataclass(frozen=True)
class NeverDeclared:
    value: int


@contextmanager
def _receiver_registered(key: str, event_class: type):
    entry = RegisteredReceiver(
        key=key,
        event_class=event_class,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
    )
    registry.register_receiver(entry)
    try:
        yield
    finally:
        registry._receivers.pop(key, None)


def test_a_receiver_for_an_undeclared_event_is_an_error() -> None:
    """Nothing else would ever say so: the event cannot be fired, so there is no
    failure to observe, only silence."""
    with _receiver_registered("testapp.dangling", NeverDeclared):
        problems = checks.check_receivers_have_events()
    assert [p.id for p in problems] == ["django_domain_events.E001"]
    assert "NeverDeclared" in problems[0].msg


def test_receivers_all_matched_is_clean() -> None:
    assert checks.check_receivers_have_events() == []


def test_an_importable_codec_is_clean() -> None:
    assert checks.check_codec_dependency_is_installed() == []


def test_an_unimportable_codec_is_reported_at_startup(settings) -> None:
    """A codec is imported lazily, so without this the first symptom of a
    missing extra is a delivery failing in a worker."""
    settings.DJANGO_DOMAIN_EVENTS = {"CODEC": "django_domain_events.codecs.nope.NoSuchCodec"}
    problems = checks.check_codec_dependency_is_installed()
    assert [p.id for p in problems] == ["django_domain_events.E002"]
    assert "dacite" in problems[0].hint


@pytest.mark.django_db(transaction=True)
def test_pending_rows_for_a_deleted_receiver_are_reported(
    order: OrderPlaced, record: list[str]
) -> None:
    """The cost of freezing the receiver set at fire time, surfaced as a question
    rather than found in a log."""
    with transaction.atomic():
        fire(order)

    removed = registry._receivers.pop("testapp.durable_receiver")
    try:
        problems = checks.check_no_orphaned_deliveries()
    finally:
        registry._receivers["testapp.durable_receiver"] = removed

    assert [p.id for p in problems] == ["django_domain_events.W001"]
    assert "testapp.durable_receiver" in problems[0].msg


@pytest.mark.django_db(transaction=True)
def test_no_pending_rows_is_clean() -> None:
    assert checks.check_no_orphaned_deliveries() == []
