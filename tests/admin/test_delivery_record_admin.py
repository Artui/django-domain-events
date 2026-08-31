"""Tests mirroring ``django_domain_events/admin/delivery_record_admin.py``."""

from __future__ import annotations

import pytest
from django.contrib import admin
from django.contrib.messages.storage.fallback import FallbackStorage
from django.db import transaction
from django.test import RequestFactory

from django_domain_events.admin.delivery_record_admin import DeliveryRecordAdmin
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db


def _request():
    request = RequestFactory().get("/admin/")
    request.session = {}
    request._messages = FallbackStorage(request)
    return request


def _admin() -> DeliveryRecordAdmin:
    return admin.site._registry[DeliveryRecord]


def test_django_autodiscovery_registers_our_model_admin() -> None:
    assert isinstance(admin.site._registry[DeliveryRecord], DeliveryRecordAdmin)


def test_deliveries_cannot_be_added_changed_or_deleted() -> None:
    """Editing ``status`` by hand is how a claimed row gets handed to a second
    worker."""
    site, request = _admin(), _request()
    assert site.has_add_permission(request) is False
    assert site.has_change_permission(request) is False
    assert site.has_delete_permission(request) is False
    assert set(site.get_readonly_fields(request)) == {f.name for f in DeliveryRecord._meta.fields}


def test_requeue_resets_the_budget_rather_than_only_the_status(
    order: OrderPlaced, record: list[str]
) -> None:
    """Through requeue_dead, not queryset.update(): a status flip alone leaves
    rows that dead-letter again on the first failure."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.DEAD, attempts=5, claimed_by="w1", last_error="boom"
    )

    request = _request()
    _admin().requeue(request, DeliveryRecord.objects.all())

    row = DeliveryRecord.objects.first()
    assert (row.status, row.attempts, row.claimed_by, row.last_error) == (
        DeliveryStatus.PENDING,
        0,
        "",
        "",
    )
    assert list(request._messages)[0].message == "Requeued 2 deliveries."


def test_a_mixed_selection_reports_what_it_left_alone(
    order: OrderPlaced, record: list[str]
) -> None:
    """Counted from one list captured before the update. Asking the queryset
    afterwards re-runs it against rows this call just moved to PENDING, and
    reports every requeued row as skipped."""
    with transaction.atomic():
        fire(order)
    dead = DeliveryRecord.objects.order_by("pk").first()
    DeliveryRecord.objects.filter(pk=dead.pk).update(status=DeliveryStatus.DEAD, attempts=5)

    request = _request()
    _admin().requeue(request, DeliveryRecord.objects.all())

    assert (
        list(request._messages)[0].message
        == "Requeued 1 deliveries. 1 were not dead and were left alone."
    )


def test_a_selection_with_nothing_dead_says_so(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    request = _request()
    _admin().requeue(request, DeliveryRecord.objects.all())
    assert (
        list(request._messages)[0].message
        == "Requeued 0 deliveries. 2 were not dead and were left alone."
    )
