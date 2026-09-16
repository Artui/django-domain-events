"""Tests mirroring ``django_domain_events/delivery/permanent_failure.py``.

The class is a marker, so what these test is what the relay does on reading it.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from django.db import transaction

import django_domain_events
from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.permanent_failure import PermanentFailure
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    """Take this file's ad-hoc receivers back out of the process-wide registry."""
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _fire() -> None:
    with transaction.atomic():
        fire(Unheard(value=1))


def test_it_is_exported_from_the_package_root() -> None:
    assert django_domain_events.PermanentFailure is PermanentFailure


def test_it_dead_letters_on_the_attempt_that_raised_it() -> None:
    """Attempt one of five, and the row is dead.

    ``limit=1`` is one claim and one attempt, so a row that came back ``FAILED``
    cannot be retried inside this call and reach ``DEAD`` by spending its
    budget - which would read, from the status alone, like the same outcome.
    """
    seen: list[DeliveryFailure] = []

    def gone(event: Unheard) -> None:
        raise PermanentFailure("410 Gone: the customer deleted this endpoint")

    receiver(Unheard, key="probe.gone", max_attempts=5, on_failure=seen.append)(gone)
    _fire()

    assert deliver_pending(limit=1) == {DeliveryStatus.DEAD: 1}

    row = DeliveryRecord.objects.get(receiver_key="probe.gone")
    assert (row.status, row.attempts, row.max_attempts) == (DeliveryStatus.DEAD, 1, 5)
    assert row.completed_at is not None
    assert row.last_error == "PermanentFailure: 410 Gone: the customer deleted this endpoint"
    # The hook sees DEAD, exactly as it does when the budget is spent, so a
    # consumer's failure log needs no second case to recognise it.
    assert [(failure.status, failure.attempt) for failure in seen] == [(DeliveryStatus.DEAD, 1)]


def test_an_ordinary_exception_saying_the_same_thing_is_still_retried() -> None:
    """The type is the signal, never the words.

    Pins that the relay is not reading the message: a receiver that raises a
    ``RuntimeError`` about a 410 has said nothing about whether to retry.
    """

    def vague(event: Unheard) -> None:
        raise RuntimeError("410 Gone: the customer deleted this endpoint")

    receiver(Unheard, key="probe.vague", max_attempts=5)(vague)
    _fire()

    assert deliver_pending(limit=1) == {DeliveryStatus.FAILED: 1}
    row = DeliveryRecord.objects.get(receiver_key="probe.vague")
    assert (row.status, row.attempts, row.completed_at) == (DeliveryStatus.FAILED, 1, None)


def test_a_subclass_is_terminal_too() -> None:
    """A consumer naming its own reasons keeps the behaviour."""

    class EndpointGone(PermanentFailure): ...

    def gone(event: Unheard) -> None:
        raise EndpointGone("deleted")

    receiver(Unheard, key="probe.subclass")(gone)
    _fire()

    assert deliver_pending(limit=1) == {DeliveryStatus.DEAD: 1}
    assert DeliveryRecord.objects.get(receiver_key="probe.subclass").attempts == 1
