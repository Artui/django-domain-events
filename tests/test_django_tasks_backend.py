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


def test_the_adapter_falls_back_to_the_backport() -> None:
    """Core gained django.tasks in 6.0; below that the backport is the only
    path. Forcing the fallback here rather than relying on the interpreter's
    Django means both branches are covered on every version in the matrix -
    otherwise the branch that does not apply is uncovered, and the coverage gate
    fails on exactly the versions the other branch is for.
    """
    import sys
    from unittest import mock

    from django_domain_events.django_tasks_backend import _task

    with mock.patch.dict(sys.modules, {"django.tasks": None}):
        assert _task().__module__.startswith("django_tasks")


def test_the_adapter_prefers_core_when_it_is_there() -> None:
    """A project that has moved past 6.0 should not keep resolving a backport it
    no longer needs."""
    import django

    from django_domain_events.django_tasks_backend import _task

    if django.VERSION < (6, 0):
        pytest.skip("core has no django.tasks below 6.0")
    assert _task().__module__.startswith("django.tasks")


def test_every_delivery_path_honours_the_site(
    order: OrderPlaced, record: list[str], settings
) -> None:
    """dispatch_one was reachable only from run_relay, so deliver_pending,
    drain_outbox and the eager path all ran a site='task' receiver in process.

    drain_outbox is the sharpest of the three: its docstring promises it "runs
    the same claim, encode, decode and acknowledgement as the relay", so a
    consumer's tests took a path production does not - the exact failure that
    docstring exists to prevent.
    """
    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": "tests.test_django_tasks_backend.RecordingBackend"
    }
    from django_domain_events.drain_outbox import drain_outbox
    from django_domain_events.registry import registry

    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "site", "task")
    try:
        with transaction.atomic():
            fire(order)
        record.clear()
        drain_outbox()
    finally:
        object.__setattr__(entry, "site", "relay")

    assert len(ENQUEUED) == 1
    assert "durable:7" not in record


def test_a_task_site_with_no_backend_refuses(order: OrderPlaced, record: list[str]) -> None:
    """Silently running in the relay makes the declaration a lie, and the only
    symptom is work happening in the wrong process."""
    from django.core.exceptions import ImproperlyConfigured

    from django_domain_events.registry import registry

    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "site", "task")
    try:
        with transaction.atomic():
            fire(order)
        delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
            receiver_key="testapp.durable_receiver"
        )
        with pytest.raises(ImproperlyConfigured, match="no TASK_BACKEND"):
            dispatch_one(delivery_id)
    finally:
        object.__setattr__(entry, "site", "relay")


def test_a_broken_backend_does_not_break_relay_site_receivers(
    order: OrderPlaced, record: list[str], settings
) -> None:
    """The backend is built only for a receiver that asked for one, so a typo in
    TASK_BACKEND cannot break receivers that never wanted it."""
    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": "nope.NotThere"}
    with transaction.atomic():
        fire(order)
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.durable_receiver"
    )
    record.clear()

    assert dispatch_one(delivery_id) is DeliveryStatus.SUCCEEDED
    assert record == ["durable:7"]


def test_a_backend_can_be_configured_with_options(settings) -> None:
    """A dotted path alone gives the constructor no arguments, so a backend with
    any options is unreachable through the documented setting."""
    from django_domain_events.settings import get_task_backend

    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": {
            "BACKEND": "django_domain_events.django_tasks_backend.DjangoTasksBackend",
            "queue_name": "events",
        }
    }
    assert get_task_backend().queue_name == "events"
