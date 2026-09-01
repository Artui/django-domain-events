"""The changelist's cost, and the filters that keep it down.

An event log's admin is the page an operator opens during an incident, on the
table this package tells them will become the largest in their database. A
filter that builds its options with SELECT DISTINCT over that table is a full
scan on every page load, and nothing about the rendered page says so.
"""

from __future__ import annotations

import pytest
from django.contrib.auth.models import Permission, User
from django.db import connection, transaction
from django.test import Client
from django.test.utils import CaptureQueriesContext

from django_domain_events.admin.event_name_filter import EventNameFilter
from django_domain_events.admin.receiver_key_filter import ReceiverKeyFilter
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from tests.testapp.events import OrderPlaced, Unheard

pytestmark = pytest.mark.django_db

EVENTS_URL = "/admin/django_domain_events/eventrecord/"
DELIVERIES_URL = "/admin/django_domain_events/deliveryrecord/"


def _viewer(*codenames: str) -> Client:
    user = User.objects.create_user("viewer", password="pw", is_staff=True)
    for codename in codenames:
        user.user_permissions.add(Permission.objects.get(codename=codename))
    client = Client()
    client.force_login(user)
    return client


def _captured() -> CaptureQueriesContext:
    """Capture around the request itself.

    A ``django_assert_num_queries`` block that closes before the client call
    records nothing, and every assertion over its captured queries then passes
    for the wrong reason.
    """
    return CaptureQueriesContext(connection)


def _scans(queries, table: str) -> list[str]:
    return [
        q["sql"]
        for q in queries
        if table in q["sql"] and ("DISTINCT" in q["sql"] or "MIN(" in q["sql"])
    ]


def test_the_event_changelist_scans_nothing(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    client = _viewer("view_eventrecord")
    with _captured() as captured:
        assert client.get(EVENTS_URL).status_code == 200
    assert _scans(captured.captured_queries, EventRecord._meta.db_table) == []


def test_the_delivery_changelist_does_not_touch_available_at(
    order: OrderPlaced, record: list[str]
) -> None:
    """The model keeps no plain index on that column on purpose: "a second full
    index on it would be written on every insert and read never"."""
    with transaction.atomic():
        fire(order)
    client = _viewer("view_deliveryrecord")
    with _captured() as captured:
        assert client.get(DELIVERIES_URL).status_code == 200
    aggregates = [
        q["sql"]
        for q in captured.captured_queries
        if "available_at" in q["sql"] and ("DISTINCT" in q["sql"] or "MIN(" in q["sql"])
    ]
    assert aggregates == []


def test_the_event_filter_lists_declarations_not_rows() -> None:
    """A declared event nobody has fired appears, and selecting it shows the
    empty result that is itself the finding."""
    options = dict(EventNameFilter(None, {}, EventRecord, None).lookups(None, None))
    assert "testapp.Unheard" in options
    assert not EventRecord.objects.filter(name="testapp.Unheard").exists()


def test_the_receiver_filter_lists_declarations_not_rows() -> None:
    options = dict(ReceiverKeyFilter(None, {}, DeliveryRecord, None).lookups(None, None))
    assert "testapp.durable_receiver" in options


def test_selecting_an_event_narrows_the_list(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
        fire(Unheard(value=1))
    client = _viewer("view_eventrecord")

    unfiltered = client.get(EVENTS_URL).context["cl"].queryset
    assert unfiltered.count() == 2

    filtered = client.get(EVENTS_URL, {"event_name": "testapp.OrderPlaced"}).context["cl"].queryset
    assert [e.name for e in filtered] == ["testapp.OrderPlaced"]


def test_selecting_a_receiver_narrows_the_deliveries(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    client = _viewer("view_deliveryrecord")

    assert client.get(DELIVERIES_URL).context["cl"].queryset.count() == 2
    filtered = (
        client.get(DELIVERIES_URL, {"receiver": "testapp.durable_receiver"}).context["cl"].queryset
    )
    assert [d.receiver_key for d in filtered] == ["testapp.durable_receiver"]
