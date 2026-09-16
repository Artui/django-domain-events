"""Tests mirroring ``django_domain_events/declaration/any_event.py``.

The class is a marker, so what these test is what declaring a receiver for it
does: the wildcard is matched when an event is fired, never expanded when the
receiver is declared.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator

import pytest
from django.conf import settings
from django.db import transaction
from django.test.utils import override_settings

import django_domain_events
from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from tests.testapp.events import PinnedName, Unheard

pytestmark = pytest.mark.django_db

LATE_EVENT = "lateapp.LateArrival"
LATE_MODULE = "tests.lateapp.events"


@pytest.fixture(autouse=True)
def _no_leaked_declarations() -> Iterator[None]:
    """Put the process-wide registry back as this file found it.

    Includes the late app's event and its module: left registered, the event
    would appear in every catalogue test that runs afterwards, and left in
    ``sys.modules`` a second run would import nothing and declare nothing.
    """
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)
    late = registry.event_for_name(LATE_EVENT)
    if late is not None:
        registry._events_by_name.pop(LATE_EVENT)
        registry._events_by_class.pop(late.event_class)
    sys.modules.pop(LATE_MODULE, None)


def test_it_is_exported_from_the_package_root() -> None:
    assert django_domain_events.AnyEvent is AnyEvent


def test_a_wildcard_is_owed_every_event_it_is_fired_with() -> None:
    receiver(AnyEvent, key="probe.everything")(lambda event: None)

    with transaction.atomic():
        fire(Unheard(value=1))
        fire(PinnedName(value=2))

    rows = DeliveryRecord.objects.filter(receiver_key="probe.everything").order_by("pk")
    assert [row.event.name for row in rows] == ["testapp.Unheard", "testapp.pinned"]


def test_it_is_handed_the_event_that_was_fired() -> None:
    received: list[object] = []
    receiver(AnyEvent, key="probe.everything")(received.append)

    with transaction.atomic():
        fire(Unheard(value=7))
    drain_outbox()

    assert received == [Unheard(value=7)]


def test_an_event_declared_by_an_app_that_loads_later_still_gets_a_row() -> None:
    """The failure the registry walk has, exercised through real app loading.

    The wildcard is declared first. Only then is ``tests.lateapp`` installed, so
    its ``events.py`` is imported by the substrate's own autodiscovery and the
    event comes into existence afterwards. A wildcard expanded to one receiver
    per event at declaration time has nothing to expand to for it, and writes
    no row - which is what the precondition below pins, so this cannot pass by
    the event having been declared up front after all.
    """
    received: list[object] = []
    receiver(AnyEvent, key="probe.everything")(received.append)
    assert registry.event_for_name(LATE_EVENT) is None, "declared before the wildcard"
    assert LATE_MODULE not in sys.modules

    with override_settings(INSTALLED_APPS=[*settings.INSTALLED_APPS, "tests.lateapp"]):
        late = registry.event_for_name(LATE_EVENT)
        assert late is not None, "autodiscovery declared it when the app was installed"

        with transaction.atomic():
            event_id = fire(late.event_class(value=3))

        row = DeliveryRecord.objects.get(receiver_key="probe.everything")
        assert (row.event.pk, row.event.name, row.target) == (event_id, LATE_EVENT, "")

        drain_outbox()
        assert received == [late.event_class(value=3)]
