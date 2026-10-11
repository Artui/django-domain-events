"""Tests mirroring ``django_domain_events/admin/delivery_record_admin.py``."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest
from django.contrib import admin
from django.contrib.messages.storage.fallback import FallbackStorage
from django.db import transaction
from django.test import RequestFactory

from django_domain_events.admin.delivery_record_admin import DeliveryRecordAdmin
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
    # Every column, with the digest swapped for its hex rendering.
    columns = {f.name for f in DeliveryRecord._meta.fields}
    assert set(site.get_readonly_fields(request)) == columns - {"target_digest"} | {"digest"}


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


def test_cancel_withdraws_the_selected_rows_and_reports(
    order: OrderPlaced, record: list[str]
) -> None:
    """Through cancel_deliveries, scoped to the selected rows' own events: a
    selection spanning two events cancels in both."""
    with transaction.atomic():
        fire(order)
        fire(order)

    request = _request()
    _admin().cancel(request, DeliveryRecord.objects.all())

    assert DeliveryRecord.objects.filter(status=DeliveryStatus.CANCELLED).count() == 4
    assert all(row.completed_at for row in DeliveryRecord.objects.all())
    assert list(request._messages)[0].message == "Cancelled 4 deliveries."


def test_cancel_touches_only_the_selected_rows_of_an_event(
    order: OrderPlaced, record: list[str]
) -> None:
    """The rows are passed as ``delivery_ids``, not widened to their events:
    selecting one delivery must not withdraw its sibling receiver's."""
    with transaction.atomic():
        fire(order)
    chosen = DeliveryRecord.objects.order_by("pk").first()

    request = _request()
    _admin().cancel(request, DeliveryRecord.objects.filter(pk=chosen.pk))

    assert list(DeliveryRecord.objects.order_by("pk").values_list("status", flat=True)) == [
        DeliveryStatus.CANCELLED,
        DeliveryStatus.PENDING,
    ]
    assert list(request._messages)[0].message == "Cancelled 1 deliveries."


def test_cancel_reports_a_claimed_row_as_in_flight(order: OrderPlaced, record: list[str]) -> None:
    """Three cancelled and one in flight, so the two counts cannot be read from
    each other and still produce this message."""
    with transaction.atomic():
        fire(order)
        fire(order)
    claimed = DeliveryRecord.objects.order_by("pk").first()
    DeliveryRecord.objects.filter(pk=claimed.pk).update(status=DeliveryStatus.CLAIMED)

    request = _request()
    _admin().cancel(request, DeliveryRecord.objects.all())

    assert DeliveryRecord.objects.get(pk=claimed.pk).status == DeliveryStatus.CLAIMED
    assert (
        list(request._messages)[0].message
        == "Cancelled 3 deliveries. 1 were in flight and were left to run."
    )


def test_cancel_reports_settled_rows_it_left_alone(order: OrderPlaced, record: list[str]) -> None:
    """Counted from one list captured before the call, as requeue counts. The
    selection is a filter the cancel changes the answer to - "not cancelled",
    as a status filter in the changelist would be - so asking the queryset
    afterwards would drop the row this call just cancelled and misreport the
    settled count."""
    with transaction.atomic():
        fire(order)
    dead, claimed, done = DeliveryStatus.DEAD, DeliveryStatus.CLAIMED, DeliveryStatus.SUCCEEDED
    first = DeliveryRecord.objects.order_by("pk").first()
    DeliveryRecord.objects.filter(pk=first.pk).update(status=dead)
    with transaction.atomic():
        fire(order)
    rows = list(DeliveryRecord.objects.exclude(pk=first.pk).order_by("pk"))
    DeliveryRecord.objects.filter(pk=rows[0].pk).update(status=claimed)
    DeliveryRecord.objects.filter(pk=rows[1].pk).update(status=done)

    request = _request()
    _admin().cancel(request, DeliveryRecord.objects.exclude(status=DeliveryStatus.CANCELLED))

    assert list(request._messages)[0].message == (
        "Cancelled 1 deliveries. 1 were in flight and were left to run."
        " 2 were already settled and were left alone."
    )


def test_cancelled_rows_can_be_listed_on_their_own(admin_client) -> None:
    """The status filter is the field's choices, so a cancelled row is one an
    operator can list by itself, labelled as such, the way dead letters are."""
    event = EventRecord.objects.create(
        name="testapp.Unheard",
        version=1,
        payload={"value": 1},
        occurred_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )
    for key, status in (("probe.kept", DeliveryStatus.PENDING), ("probe.off", "cancelled")):
        DeliveryRecord.objects.create(
            event=event, receiver_key=key, status=status, available_at=event.recorded_at
        )

    response = admin_client.get(
        "/admin/django_domain_events/deliveryrecord/", {"status__exact": "cancelled"}
    )

    assert response.status_code == 200
    assert [row.receiver_key for row in response.context["cl"].result_list] == ["probe.off"]
    assert "Cancelled" in response.content.decode()


def test_a_fan_out_delivery_can_be_found_by_its_target(admin_client) -> None:
    """What an operator holds when a fan-out goes wrong is the target: "endpoint
    42 says it never got it"."""
    event = EventRecord.objects.create(
        name="testapp.Unheard",
        version=1,
        payload={"value": 1},
        occurred_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )
    for target in ("endpoint-42", "endpoint-7"):
        DeliveryRecord.objects.create(
            event=event, receiver_key="probe.fan", target=target, available_at=event.recorded_at
        )

    response = admin_client.get("/admin/django_domain_events/deliveryrecord/", {"q": "endpoint-42"})

    assert response.status_code == 200
    rows = list(response.context["cl"].result_list)
    assert [row.target for row in rows] == ["endpoint-42"]
    assert "endpoint-42" in response.content.decode()


def test_the_detail_page_shows_the_digest_as_hex(admin_client) -> None:
    """Stored as raw bytes, which Django renders as a Python ``b'...'`` literal
    full of escapes. Hex is what an operator can compare against a digest
    computed anywhere else."""
    event = EventRecord.objects.create(
        name="testapp.Unheard",
        version=1,
        payload={"value": 1},
        occurred_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="probe.fan", target="endpoint-42", available_at=event.recorded_at
    )

    response = admin_client.get(f"/admin/django_domain_events/deliveryrecord/{row.pk}/change/")

    assert response.status_code == 200
    page = response.content.decode()
    assert hashlib.sha256(b"endpoint-42").hexdigest() in page
    assert "b&#x27;" not in page


def test_cancel_goes_through_the_operation(
    order: OrderPlaced, record: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The action calls cancel_deliveries rather than updating the rows itself:
    the operation's event lock is what makes a cancel wait for an open replay,
    which no single-connection outcome here would notice missing."""
    with transaction.atomic():
        event_id = fire(order)
    chosen = DeliveryRecord.objects.order_by("pk").first()
    calls: list[tuple[list[int], list[int]]] = []

    def spy(event_ids, *, receiver_key=None, delivery_ids=None):
        calls.append((list(event_ids), list(delivery_ids)))
        return cancel_deliveries(event_ids, receiver_key=receiver_key, delivery_ids=delivery_ids)

    monkeypatch.setattr("django_domain_events.admin.delivery_record_admin.cancel_deliveries", spy)
    _admin().cancel(_request(), DeliveryRecord.objects.filter(pk=chosen.pk))

    assert calls == [([event_id], [chosen.pk])]
