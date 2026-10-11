"""Tests mirroring ``django_domain_events/admin/event_record_admin.py``."""

from __future__ import annotations

import pytest
from django.contrib import admin
from django.contrib.messages.storage.fallback import FallbackStorage
from django.db import transaction
from django.test import RequestFactory

from django_domain_events.admin.event_record_admin import EventRecordAdmin
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.cancel_deliveries import cancel_deliveries
from django_domain_events.types.delivery_status import DeliveryStatus
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


def test_a_cancelled_delivery_is_not_owed(order: OrderPlaced, record: list[str]) -> None:
    """The column counts what is still owed, and a cancelled row never will
    be: counted, the event would read as waiting on work nobody will do."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        status=DeliveryStatus.CANCELLED
    )
    site = _admin()
    row = site.get_queryset(_request()).get()
    assert site.owed(row) == 1


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


def test_cancel_withdraws_every_owed_delivery_of_the_events_and_reports(
    order: OrderPlaced, record: list[str]
) -> None:
    """Through cancel_deliveries, across every receiver of the selected events:
    the owed rows are cancelled, the claimed one is left to run and counted, a
    settled one keeps its outcome, and an event not selected is untouched. Two
    cancelled and one in flight, so neither count can stand in for the other."""
    with transaction.atomic():
        chosen = fire(order)
        other = fire(order)
    rows = list(DeliveryRecord.objects.filter(event=chosen).order_by("pk"))
    DeliveryRecord.objects.filter(pk=rows[0].pk).update(status=DeliveryStatus.CLAIMED)
    DeliveryRecord.objects.create(
        event_id=chosen,
        receiver_key="probe.done",
        status=DeliveryStatus.SUCCEEDED,
        available_at=rows[0].available_at,
    )
    DeliveryRecord.objects.create(
        event_id=chosen,
        receiver_key="probe.pending",
        status=DeliveryStatus.PENDING,
        available_at=rows[0].available_at,
    )

    request = _request()
    _admin().cancel(request, EventRecord.objects.filter(pk=chosen))

    statuses = dict(
        DeliveryRecord.objects.filter(event=chosen).values_list("receiver_key", "status")
    )
    assert statuses == {
        rows[0].receiver_key: DeliveryStatus.CLAIMED,
        rows[1].receiver_key: DeliveryStatus.CANCELLED,
        "probe.done": DeliveryStatus.SUCCEEDED,
        "probe.pending": DeliveryStatus.CANCELLED,
    }
    assert not DeliveryRecord.objects.filter(event=other, status=DeliveryStatus.CANCELLED).exists()
    message = list(request._messages)[0].message
    assert message == "Cancelled 2 deliveries and left 1 in flight to run."


def test_cancel_goes_through_the_operation(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The action calls cancel_deliveries rather than updating the rows itself:
    the operation's event lock is what makes a cancel wait for an open replay,
    which no single-connection outcome here would notice missing."""
    with transaction.atomic():
        chosen = fire(order)
        fire(order)
    calls: list[list[int]] = []

    def spy(event_ids, *, receiver_key=None, delivery_ids=None):
        calls.append(list(event_ids))
        return cancel_deliveries(event_ids, receiver_key=receiver_key, delivery_ids=delivery_ids)

    monkeypatch.setattr("django_domain_events.admin.event_record_admin.cancel_deliveries", spy)
    _admin().cancel(_request(), EventRecord.objects.filter(pk=chosen))

    assert calls == [[chosen]]
