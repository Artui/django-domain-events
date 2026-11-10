from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Iterator
from datetime import timedelta
from unittest import mock

import pytest
from django.db import connection, connections, transaction
from django.utils import timezone as django_timezone

from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.django_tasks_backend import deliver_delivery
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import receiver_deleted, receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def _clear_enqueued():
    ENQUEUED.clear()
    MESSAGES.clear()
    yield
    ENQUEUED.clear()
    MESSAGES.clear()


ENQUEUED: list[int] = []
MESSAGES: list[tuple[tuple[object, ...], dict[str, object]]] = []

BACKEND = "tests.delivery.test_django_tasks_backend.RecordingBackend"


class RecordingBackend:
    """A task backend is a one-method protocol, so a stand-in is cheap."""

    def __init__(self) -> None:
        self.enqueued = ENQUEUED

    def enqueue(self, *args: object, **kwargs: object) -> None:
        self.enqueued.append(args[0])
        # Carried through JSON, the one form every queue can carry.
        MESSAGES.append(json.loads(json.dumps([args, kwargs])))


@pytest.fixture
def task_site(settings) -> Iterator[None]:
    """``testapp.durable_receiver`` declared ``site="task"``, with a recording
    backend configured to receive what the relay hands off."""
    from django_domain_events.declaration.registry import registry

    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": BACKEND}
    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "site", "task")
    try:
        yield
    finally:
        object.__setattr__(entry, "site", "relay")


def _replay(message: list) -> None:
    """Run the task body on a message, as the worker that dequeued it would."""
    args, kwargs = message
    deliver_delivery(*args, **kwargs)


def _handed_off(order: OrderPlaced, worker_id: str = "w1") -> tuple[int, list]:
    """Fire, claim and dispatch as the relay does; return the row and message."""
    with transaction.atomic():
        fire(order)
    claim_batch(
        worker_id=worker_id, now=django_timezone.now(), lease=timedelta(minutes=5), limit=10
    )
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.durable_receiver"
    )
    assert dispatch_one(delivery_id, worker_id=worker_id) is None
    assert [delivery_id] == ENQUEUED
    return delivery_id, MESSAGES[0]


def test_a_relay_site_receiver_is_delivered_in_place(
    order: OrderPlaced, record: list[str], settings
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {
        # The dotted path names this module, so it has to move with the file.
        "TASK_BACKEND": "tests.delivery.test_django_tasks_backend.RecordingBackend"
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
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """The row stays claimed under its lease, with no outcome counted yet."""
    delivery_id, message = _handed_off(order)

    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert row.status == DeliveryStatus.CLAIMED
    assert message == [
        [delivery_id],
        {"claimed_by": "w1", "claimed_at": row.claimed_at.isoformat()},
    ]
    assert "durable:7" not in record


def test_an_unclaimed_row_is_not_enqueued(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """A PENDING row has no claim, so the task could never take it."""
    with transaction.atomic():
        fire(order)
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.durable_receiver"
    )
    record.clear()

    assert dispatch_one(delivery_id) is None

    assert ENQUEUED == []
    assert record == []


def test_the_task_body_delivers_the_row(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """A backend is found by dotted path in a worker, so no closure or method."""
    delivery_id, message = _handed_off(order)
    record.clear()

    _replay(message)

    assert record == ["durable:7"]
    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert row.status == DeliveryStatus.SUCCEEDED
    assert row.claimed_by.startswith("task-"), "the task runs under its own claim"


def test_the_django_tasks_adapter_enqueues(
    order: OrderPlaced, record: list[str], settings, task_site: None
) -> None:
    """The framework is not a dependency, so the import is lazy."""
    settings.TASKS = {"default": {"BACKEND": "django_tasks.backends.immediate.ImmediateBackend"}}
    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": "django_domain_events.delivery.django_tasks_backend.DjangoTasksBackend"
    }
    with transaction.atomic():
        fire(order)
    claim_batch(worker_id="w1", now=django_timezone.now(), lease=timedelta(minutes=5), limit=10)
    delivery_id = DeliveryRecord.objects.values_list("pk", flat=True).get(
        receiver_key="testapp.durable_receiver"
    )

    record.clear()
    assert dispatch_one(delivery_id, worker_id="w1") is None

    assert record == ["durable:7"]
    assert (
        DeliveryRecord.objects.values_list("status", flat=True).get(pk=delivery_id)
        == DeliveryStatus.SUCCEEDED
    )


def test_the_adapter_falls_back_to_the_backport() -> None:
    """Core gained django.tasks in 6.0; below that the backport is the only path."""
    import sys
    from unittest import mock

    from django_domain_events.delivery.django_tasks_backend import _task

    with mock.patch.dict(sys.modules, {"django.tasks": None}):
        assert _task().__module__.startswith("django_tasks")


def test_the_adapter_prefers_core_when_it_is_there() -> None:
    """A project that has moved past 6.0 should not keep resolving a backport it no longer needs."""
    import django

    from django_domain_events.delivery.django_tasks_backend import _task

    if django.VERSION < (6, 0):
        pytest.skip("core has no django.tasks below 6.0")
    assert _task().__module__.startswith("django.tasks")


def test_every_delivery_path_honours_the_site(
    order: OrderPlaced, record: list[str], settings
) -> None:
    """Other paths ran a site='task' receiver in process, not on the task backend."""
    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": "tests.delivery.test_django_tasks_backend.RecordingBackend"
    }
    from django_domain_events.declaration.registry import registry
    from django_domain_events.delivery.drain_outbox import drain_outbox

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
    """Silently running in the relay makes the declaration a lie."""
    from django.core.exceptions import ImproperlyConfigured

    from django_domain_events.declaration.registry import registry

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
    """The backend is built only for receivers that asked for one."""
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
    """A dotted path alone passes no arguments, so options are unreachable."""
    from django_domain_events.settings import get_task_backend

    settings.DJANGO_DOMAIN_EVENTS = {
        "TASK_BACKEND": {
            "BACKEND": "django_domain_events.delivery.django_tasks_backend.DjangoTasksBackend",
            "queue_name": "events",
        }
    }
    assert get_task_backend().queue_name == "events"


@pytest.mark.django_db
def test_the_task_site_extends_the_lease_before_enqueueing(
    order: OrderPlaced, record: list[str]
) -> None:
    """The path where the wait is the point: the row must survive the queue's backlog."""
    from datetime import timedelta

    from django_domain_events.delivery.claim_batch import claim_batch
    from django_domain_events.delivery.deliver import dispatch_one
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.types.delivery_mode import DeliveryMode
    from django_domain_events.types.registered_receiver import RegisteredReceiver
    from tests.conftest import receiver_registered

    enqueued: list[int] = []

    class Recording:
        def enqueue(self, delivery_id: int, claimed_by: str, claimed_at: str) -> None:
            enqueued.append(delivery_id)

    entry = RegisteredReceiver(
        key="testapp.durable_receiver",
        event_class=OrderPlaced,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="task",
        lease_seconds=2400,
    )
    with transaction.atomic():
        fire(order)
    row = DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").get()
    started = django_timezone.now()
    claim_batch(worker_id="w1", now=started, lease=timedelta(seconds=5), limit=10)

    with (
        receiver_deleted("testapp.durable_receiver"),
        receiver_registered(entry),
        mock.patch("django_domain_events.delivery.deliver.get_task_backend", lambda: Recording()),
    ):
        assert dispatch_one(row.pk, worker_id="w1") is None

    assert enqueued == [row.pk]
    assert DeliveryRecord.objects.get(pk=row.pk).lease_expires_at > started + timedelta(
        seconds=1200
    )


@pytest.mark.django_db
def test_a_lost_row_is_not_enqueued(order: OrderPlaced, record: list[str]) -> None:
    """Enqueueing a row another worker holds hands it to two places."""
    from django_domain_events.delivery.deliver import dispatch_one
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.types.delivery_mode import DeliveryMode
    from django_domain_events.types.delivery_status import DeliveryStatus
    from django_domain_events.types.registered_receiver import RegisteredReceiver
    from tests.conftest import receiver_registered

    enqueued: list[int] = []

    class Recording:
        def enqueue(self, delivery_id: int, claimed_by: str, claimed_at: str) -> None:
            enqueued.append(delivery_id)

    entry = RegisteredReceiver(
        key="testapp.durable_receiver",
        event_class=OrderPlaced,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="task",
    )
    with transaction.atomic():
        fire(order)
    row = DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").get()
    DeliveryRecord.objects.filter(pk=row.pk).update(
        status=DeliveryStatus.CLAIMED, claimed_by="someone-else"
    )

    with (
        receiver_deleted("testapp.durable_receiver"),
        receiver_registered(entry),
        mock.patch("django_domain_events.delivery.deliver.get_task_backend", lambda: Recording()),
    ):
        assert dispatch_one(row.pk, worker_id="w1") is None

    assert enqueued == []


def test_a_message_delivered_twice_runs_its_receiver_once(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """acks_late and visibility timeouts both redeliver a task that already ran."""
    delivery_id, message = _handed_off(order)
    record.clear()

    _replay(message)
    _replay(message)

    assert record == ["durable:7"]
    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert (row.status, row.attempts) == (DeliveryStatus.SUCCEEDED, 1)


def test_a_message_for_a_dead_row_does_not_run(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """A dead letter is final, whatever message is still in flight for it."""
    delivery_id, message = _handed_off(order)
    DeliveryRecord.objects.filter(pk=delivery_id).update(
        status=DeliveryStatus.DEAD, attempts=5, max_attempts=5
    )
    record.clear()

    _replay(message)

    assert record == []
    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert (row.status, row.attempts) == (DeliveryStatus.DEAD, 5)


def test_a_message_for_a_failed_row_not_yet_due_does_not_run(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """The message outlived the attempt it was for."""
    delivery_id, message = _handed_off(order)
    DeliveryRecord.objects.filter(pk=delivery_id).update(
        status=DeliveryStatus.FAILED,
        attempts=1,
        available_at=django_timezone.now() + timedelta(hours=1),
    )
    record.clear()

    _replay(message)

    assert record == []
    assert DeliveryRecord.objects.get(pk=delivery_id).status == DeliveryStatus.FAILED


def test_a_stale_message_does_not_run_a_row_the_relay_reclaimed(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """A CLAIMED row owed under another claim, which the not-owed check cannot see."""
    delivery_id, message = _handed_off(order)
    DeliveryRecord.objects.filter(pk=delivery_id).update(
        lease_expires_at=django_timezone.now() - timedelta(seconds=1)
    )
    claim_batch(worker_id="w2", now=django_timezone.now(), lease=timedelta(hours=1), limit=10)
    record.clear()

    _replay(message)

    assert record == []
    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert (row.status, row.claimed_by) == (DeliveryStatus.CLAIMED, "w2")


def test_a_message_naming_another_worker_does_not_take_the_row(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """Holds the claimed_by half of the take alone: only the owner moved."""
    delivery_id, message = _handed_off(order)
    DeliveryRecord.objects.filter(pk=delivery_id).update(claimed_by="someone-else")
    record.clear()

    _replay(message)

    assert record == []
    assert DeliveryRecord.objects.get(pk=delivery_id).claimed_by == "someone-else"


def test_a_message_from_an_earlier_claim_by_the_same_worker_does_not_take_the_row(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """Holds the claimed_at half alone: the same worker reclaims its own lapsed row."""
    delivery_id, message = _handed_off(order)
    later = django_timezone.now() + timedelta(seconds=1)
    DeliveryRecord.objects.filter(pk=delivery_id).update(claimed_at=later)
    record.clear()

    _replay(message)

    assert record == []
    assert DeliveryRecord.objects.get(pk=delivery_id).claimed_at == later


def test_a_message_for_a_released_row_does_not_take_it(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """Holds the status half on its own."""
    delivery_id, message = _handed_off(order)
    DeliveryRecord.objects.filter(pk=delivery_id).update(status=DeliveryStatus.PENDING)
    record.clear()

    _replay(message)

    assert record == []
    assert DeliveryRecord.objects.get(pk=delivery_id).status == DeliveryStatus.PENDING


def test_a_stale_copy_turned_away_is_logged_at_info(
    order: OrderPlaced, record: list[str], task_site: None, caplog: pytest.LogCaptureFixture
) -> None:
    """A redelivered or lapsed copy is what the take exists to turn away."""
    delivery_id, message = _handed_off(order)
    _replay(message)
    record.clear()

    with caplog.at_level(logging.INFO, logger="django_domain_events.delivery.deliver"):
        _replay(message)

    assert record == []
    refused = [r for r in caplog.records if "no longer holds" in r.getMessage()]
    assert [r.levelno for r in refused] == [logging.INFO]


@pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason=(
        "two connections racing for one row; in-memory SQLite gives each "
        "connection its own database, so there is no shared row to race for"
    ),
)
def test_two_copies_at_once_run_the_receiver_once(
    order: OrderPlaced, record: list[str], task_site: None, caplog
) -> None:
    """Two workers dequeue the same message at the same moment, each on its own connection."""
    delivery_id, message = _handed_off(order)
    ran: list[int] = []
    barrier = threading.Barrier(2)

    def slow(evt: OrderPlaced) -> None:
        ran.append(1)
        # Long enough that the other copy is past its read while this one still runs.
        time.sleep(0.3)

    errors: list[BaseException] = []

    def copy() -> None:
        try:
            barrier.wait(timeout=10)
            _replay(message)
        except BaseException as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    with (
        receiver_replaced("testapp.durable_receiver", slow),
        caplog.at_level("INFO", logger="django_domain_events.delivery.deliver"),
    ):
        threads = [threading.Thread(target=copy) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

    assert not any(t.is_alive() for t in threads), "a copy did not finish"
    assert errors == []
    refused = [r for r in caplog.records if "no longer holds" in r.getMessage()]
    assert len(refused) == 1
    assert refused[0].levelno == logging.INFO
    assert ran == [1]
    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert (row.status, row.attempts) == (DeliveryStatus.SUCCEEDED, 1)


def test_the_eager_path_hands_off_under_its_own_claim(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """The eager path claims as ``eager`` and must dispatch as ``eager``."""
    from django_domain_events.declaration.registry import registry
    from django_domain_events.delivery import claim_batch as claim_module

    real = claim_module.claim_batch

    def claim_then_lose(**kwargs: object) -> list[int]:
        ids = real(**kwargs)
        DeliveryRecord.objects.filter(pk__in=ids).update(claimed_by="thief")
        return ids

    entry = registry.receiver_for_key("testapp.durable_receiver")
    object.__setattr__(entry, "eager", True)
    try:
        with mock.patch.object(claim_module, "claim_batch", claim_then_lose), transaction.atomic():
            fire(order)
    finally:
        object.__setattr__(entry, "eager", False)

    assert ENQUEUED == []


def test_the_take_survives_a_receiver_that_raises(
    order: OrderPlaced, record: list[str], task_site: None
) -> None:
    """The take commits on its own, before the receiver's transaction opens."""
    from django_domain_events.declaration.registry import registry

    delivery_id, message = _handed_off(order)
    failures: list[object] = []

    def explode(evt: OrderPlaced) -> None:
        raise RuntimeError("downstream is down")

    entry = registry.receiver_for_key("testapp.durable_receiver")
    original = entry.on_failure
    object.__setattr__(entry, "on_failure", failures.append)
    try:
        with receiver_replaced("testapp.durable_receiver", explode):
            _replay(message)
            row = DeliveryRecord.objects.get(pk=delivery_id)
            assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
            assert len(failures) == 1

            _replay(message)
    finally:
        object.__setattr__(entry, "on_failure", original)

    row = DeliveryRecord.objects.get(pk=delivery_id)
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert len(failures) == 1
