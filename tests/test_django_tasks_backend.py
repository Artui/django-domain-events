from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.deliver import dispatch_one
from django_domain_events.django_tasks_backend import DjangoTasksBackend, deliver_delivery
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def _clear_enqueued():
    ENQUEUED.clear()
    yield
    ENQUEUED.clear()


ENQUEUED: list[int] = []


class RecordingBackend:
    """A task backend is a one-method protocol precisely so this is all it takes
    to stand in for one. Settings name a dotted path and the package builds it,
    so the record has to live outside the instance."""

    def __init__(self) -> None:
        self.enqueued = ENQUEUED

    def enqueue(self, delivery_id: int) -> None:
        self.enqueued.append(delivery_id)


def test_a_relay_site_receiver_is_delivered_in_place(
    order: OrderPlaced, record: list[str], settings
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": "tests.test_django_tasks_backend.RecordingBackend"
    }
    with transaction.atomic():
        fire(order)
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.durable_receiver"
    )
    record.clear()

    assert dispatch_one(delivery_id) is DeliveryStatus.SUCCEEDED
    assert record == ["durable:7"]
    assert ENQUEUED == []


def test_a_task_site_receiver_is_handed_off(
    order: OrderPlaced, record: list[str], settings
) -> None:
    """The row stays claimed under its lease and no outcome is counted: nothing
    has happened to it yet. If the enqueue is lost the lease lapses and the
    relay reclaims it, which is what makes a lossy queue safe here."""
    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": "tests.test_django_tasks_backend.RecordingBackend"
    }
    from django_domain_events.registry import registry

    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "site", "task")
    try:
        with transaction.atomic():
            fire(order)
        delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
            receiver_key="testapp.durable_receiver"
        )
        record.clear()
        assert dispatch_one(delivery_id) is None
    finally:
        object.__setattr__(entry, "site", "relay")

    assert [delivery_id] == ENQUEUED
    assert record == []


def test_the_task_body_delivers_the_row(order: OrderPlaced, record: list[str]) -> None:
    """A backend has to find this by dotted path in a worker process, so it
    cannot be a closure or a method."""
    with transaction.atomic():
        fire(order)
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.durable_receiver"
    )
    record.clear()

    deliver_delivery(delivery_id)
    assert record == ["durable:7"]


def test_the_django_tasks_adapter_enqueues(order: OrderPlaced, record: list[str], settings) -> None:
    """The framework is not a dependency, so the import is lazy - and it has two
    paths, because core gained django.tasks in 6.0 while the backport covers 4.2
    upward. Running this on every Django in the matrix is the point: an adapter
    only its newest supported version can execute is one nobody has tried."""
    settings.TASKS = {"default": {"BACKEND": "django_tasks.backends.immediate.ImmediateBackend"}}
    with transaction.atomic():
        fire(order)
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).first()

    record.clear()
    DjangoTasksBackend().enqueue(delivery_id)

    # The immediate backend runs it inline, so the receiver has already fired.
    assert record == ["durable:7"]
    assert (
        DeliveryRecord.objects.values_list("status", flat=True).get(pk=delivery_id)
        == DeliveryStatus.SUCCEEDED
    )
