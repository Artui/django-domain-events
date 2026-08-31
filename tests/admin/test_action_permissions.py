"""The admin actions, exercised through the real permission layer.

Calling ``site.replay(request, queryset)`` directly proves the action works and
proves nothing about who may run it: Django filters actions by
``has_<name>_permission`` when it builds the changelist, and a test that never
posts to the changelist stays green under any authorization behaviour at all.
"""

from __future__ import annotations

import pytest
from django.contrib.auth.models import Permission, User
from django.db import transaction
from django.test import Client

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db

EVENTS_URL = "/admin/django_domain_events/eventrecord/"
DELIVERIES_URL = "/admin/django_domain_events/deliveryrecord/"


def _staff(*codenames: str) -> Client:
    user = User.objects.create_user("staff", password="pw", is_staff=True)
    for codename in codenames:
        user.user_permissions.add(Permission.objects.get(codename=codename))
    client = Client()
    client.force_login(user)
    return client


def _post(client: Client, url: str, action: str, pks: list[int]):
    return client.post(
        url,
        {"action": action, "_selected_action": [str(pk) for pk in pks], "index": "0"},
        follow=True,
    )


def test_view_only_staff_cannot_replay(order: OrderPlaced, record: list[str]) -> None:
    """Replay re-runs every durable receiver: re-sent emails, re-called
    webhooks. A view grant must not carry it."""
    with transaction.atomic():
        fire(order)
    drain_outbox()

    client = _staff("view_eventrecord")
    _post(client, EVENTS_URL, "replay", [EventRecord.objects.get().pk])

    assert DeliveryRecord.objects.filter(status=DeliveryStatus.SUCCEEDED).count() == 2
    assert not DeliveryRecord.objects.filter(status=DeliveryStatus.PENDING).exists()


def test_change_permission_carries_replay(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()

    client = _staff("view_eventrecord", "change_eventrecord")
    _post(client, EVENTS_URL, "replay", [EventRecord.objects.get().pk])

    assert DeliveryRecord.objects.filter(status=DeliveryStatus.PENDING).count() == 2


def test_view_only_staff_cannot_requeue(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5)

    client = _staff("view_deliveryrecord")
    _post(
        client, DELIVERIES_URL, "requeue", list(DeliveryRecord.objects.values_list("pk", flat=True))
    )

    assert DeliveryRecord.objects.filter(status=DeliveryStatus.DEAD).count() == 2


def test_change_permission_carries_requeue(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.DEAD, attempts=5)

    client = _staff("view_deliveryrecord", "change_deliveryrecord")
    _post(
        client, DELIVERIES_URL, "requeue", list(DeliveryRecord.objects.values_list("pk", flat=True))
    )

    assert DeliveryRecord.objects.filter(status=DeliveryStatus.PENDING).count() == 2


def test_the_change_form_stays_refused_even_with_the_permission(
    order: OrderPlaced, record: list[str]
) -> None:
    """The permission carries the action, not the form. A row that can be
    edited by hand is a row the guarantee does not cover."""
    with transaction.atomic():
        fire(order)
    client = _staff("view_eventrecord", "change_eventrecord")
    pk = EventRecord.objects.get().pk
    response = client.get(f"{EVENTS_URL}{pk}/change/")
    assert b'name="_save"' not in response.content
