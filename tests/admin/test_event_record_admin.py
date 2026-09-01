"""Tests mirroring ``django_domain_events/admin/event_record_admin.py``."""

from __future__ import annotations

import pytest
from django.contrib import admin
from django.contrib.messages.storage.fallback import FallbackStorage
from django.db import transaction
from django.test import RequestFactory

from django_domain_events.admin.event_record_admin import EventRecordAdmin
from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db


def _request():
    request = RequestFactory().get("/admin/")
    request.session = {}
    request._messages = FallbackStorage(request)
    return request


def _admin() -> EventRecordAdmin:
    return admin.site._registry[EventRecord]


def test_django_autodiscovery_registers_our_model_admin() -> None:
    """The integration itself: the admin lives in a package Django imports on
    its own. A ModelAdmin built by hand in a test would pass with the package
    never loaded at all."""
    assert isinstance(admin.site._registry[EventRecord], EventRecordAdmin)


def test_the_log_cannot_be_added_changed_or_deleted() -> None:
    """The one guarantee this package sells is that a row exists if and only if
    the change committed. A form that can write one is a way to break it."""
    site, request = _admin(), _request()
    assert site.has_add_permission(request) is False
    assert site.has_change_permission(request) is False
    assert site.has_delete_permission(request) is False


def test_every_field_is_read_only() -> None:
    site = _admin()
    assert set(site.get_readonly_fields(_request())) == {f.name for f in EventRecord._meta.fields}


def test_owed_counts_only_unsettled_deliveries(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    site = _admin()
    row = site.get_queryset(_request()).get()
    assert site.owed(row) == 2

    drain_outbox()
    row = site.get_queryset(_request()).get()
    assert site.owed(row) == 0


def test_age_renders_without_a_query(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    site = _admin()
    assert site.age(EventRecord.objects.get())


def test_replay_reopens_and_reports(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    assert DeliveryRecord.objects.filter(status="succeeded").count() == 2

    request = _request()
    site = _admin()
    site.replay(request, EventRecord.objects.all())

    assert DeliveryRecord.objects.filter(status="pending").count() == 2
    message = list(request._messages)[0].message
    assert message == "Reopened 2 deliveries and added 0."
