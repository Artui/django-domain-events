"""Tests mirroring ``django_domain_events/delivery/celery_backend.py``.

None of these needs a broker. Registration is checked on a fresh app's task
registry, which is what a worker consults; the message is checked by capturing
what ``apply_async`` was given and putting it through JSON, which is what the
default serializer does to it on the way to a worker; and the whole path runs
through a Celery app in eager mode, where ``apply_async`` executes in-process.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from unittest import mock

import celery
import pytest
from celery import Celery
from django.db import transaction

from django_domain_events.delivery.celery_backend import (
    TASK_NAME,
    CeleryBackend,
    deliver_delivery_task,
)
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

BACKEND = "django_domain_events.delivery.celery_backend.CeleryBackend"


@pytest.fixture
def task_site(settings) -> Iterator[None]:
    """``testapp.durable_receiver`` declared ``site="task"``, handed to Celery."""
    from django_domain_events.declaration.registry import registry

    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": BACKEND}
    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "site", "task")
    try:
        yield
    finally:
        object.__setattr__(entry, "site", "relay")


@pytest.fixture
def eager_app() -> Iterator[Celery]:
    """A Celery app that runs ``apply_async`` in-process, current for one test.

    The previous current app is put back afterwards, because ``shared_task``
    resolves against whichever app is current at call time and an app left
    current would leak eager mode into every later test.
    """
    previous = celery.current_app._get_current_object()
    app = Celery("tests", set_as_current=False)
    app.conf.update(task_always_eager=True, task_eager_propagates=True, broker_url="memory://")
    app.set_current()
    try:
        yield app
    finally:
        previous.set_current()


def _claimed(order: OrderPlaced) -> int:
    with transaction.atomic():
        fire(order)
    claim_batch(
        worker_id="w1", now=datetime.now(timezone.utc), lease=timedelta(minutes=5), limit=10
    )
    return DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.durable_receiver"
    )


def test_a_worker_app_has_the_task_registered_by_name() -> None:
    """The trap this module's top-level import exists to avoid. A worker runs a
    message by looking its name up in its own registry; a task built lazily in
    the relay at enqueue time is never in that registry, and every eager test
    still passes. A fresh app that has only imported this module is what a
    worker with it in ``imports`` looks like."""
    app = Celery("worker", set_as_current=False)
    app.finalize()

    assert TASK_NAME in app.tasks
    # Spelled out, because the docs promise this name to anyone routing by it,
    # and a renamed constant would strand messages already on the broker.
    assert TASK_NAME == "django_domain_events.deliver_delivery"
    assert app.tasks[TASK_NAME].ignore_result is True


def test_the_message_carries_the_claim_as_json(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """What the relay hands Celery has to survive the default JSON serializer
    unchanged, and running the task body on what comes out has to deliver the
    row - exactly once, however many times the broker hands it over."""
    delivery_id = _claimed(order)
    with mock.patch("django_domain_events.delivery.celery_backend.deliver_delivery_task") as task:
        assert dispatch_one(delivery_id, worker_id="w1") is None

    (call,) = task.apply_async.call_args_list
    assert call.kwargs.keys() == {"args"}, "no queue unless one was configured"
    args = json.loads(json.dumps(call.kwargs["args"]))
    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert args == [delivery_id, "w1", row.claimed_at.isoformat()]

    record.clear()
    deliver_delivery_task(*args)
    deliver_delivery_task(*args)

    assert record == ["durable:7"]
    after = DeliveryRecord.objects.get(pk=delivery_id)
    assert (after.status, after.attempts) == (DeliveryStatus.SUCCEEDED, 1)


def test_a_configured_queue_is_named_on_the_message(settings) -> None:
    """Options reach the constructor through the mapping form of the setting."""
    from django_domain_events.settings import get_task_backend

    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": {"BACKEND": BACKEND, "queue": "events"}}
    backend = get_task_backend()
    assert isinstance(backend, CeleryBackend)

    with mock.patch("django_domain_events.delivery.celery_backend.deliver_delivery_task") as task:
        backend.enqueue(1, "w1", "2026-10-10T00:00:00+00:00")

    task.apply_async.assert_called_once_with(
        args=(1, "w1", "2026-10-10T00:00:00+00:00"), queue="events"
    )


def test_a_hand_off_runs_through_celery(
    order: OrderPlaced, record: list[str], task_site: None, eager_app: Celery
) -> None:
    """The whole path with no stand-in for Celery itself: the relay's hand-off,
    ``apply_async`` on the shared task, the current app running it, the take,
    and the delivery."""
    delivery_id = _claimed(order)
    record.clear()

    assert dispatch_one(delivery_id, worker_id="w1") is None

    assert record == ["durable:7"]
    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert row.status == DeliveryStatus.SUCCEEDED
    assert row.claimed_by.startswith("task-")
