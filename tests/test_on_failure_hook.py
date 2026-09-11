"""A receiver keeping a record of its own failure."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime, timezone

import pytest
from django.db import transaction

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    """Take this file's ad-hoc receivers back out of the process-wide registry.

    Registering one leaks, and the leak is not local: two unrelated tests
    asserting that nothing listens to ``Unheard`` went red the first time this
    file ran. Same private-dict idiom as ``receiver_registered`` in conftest,
    which exists for the same reason.
    """
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fire_and_drain() -> None:
    with transaction.atomic():
        fire(Unheard(value=1))
    for _ in range(5):
        drain_outbox()


def test_a_receivers_own_write_still_does_not_survive_its_failure() -> None:
    """The reason the hook exists, pinned so it cannot quietly stop being true.

    A receiver runs inside the transaction carrying its acknowledgement, so
    everything it writes is discarded the moment it raises. The attempts worth
    logging were exactly the ones that could not be logged.
    """
    wrote = 0

    def failing(event: Unheard) -> None:
        nonlocal wrote
        EventRecord.objects.create(
            name="probe.SideEffect", version=1, payload={}, occurred_at=_now()
        )
        wrote += 1
        raise RuntimeError("nope")

    receiver(Unheard, key="probe.inside")(failing)
    _fire_and_drain()

    assert wrote > 0, "the receiver did run and did write"
    assert EventRecord.objects.filter(name="probe.SideEffect").count() == 0


def test_the_hook_runs_and_what_it_writes_survives() -> None:
    seen: list[DeliveryFailure] = []

    def failing(event: Unheard) -> None:
        raise RuntimeError("the endpoint said no")

    def remember(failure: DeliveryFailure) -> None:
        seen.append(failure)
        EventRecord.objects.create(name="probe.Logged", version=1, payload={}, occurred_at=_now())

    receiver(Unheard, key="probe.hooked", on_failure=remember)(failing)
    _fire_and_drain()

    assert seen, "the hook was called"
    # The half that matters: written from the failure path, so it outlives the
    # rollback that discarded everything the receiver itself wrote.
    assert EventRecord.objects.filter(name="probe.Logged").count() == len(seen)


def test_it_carries_enough_to_identify_the_delivery() -> None:
    seen: list[DeliveryFailure] = []

    def failing(event: Unheard) -> None:
        raise RuntimeError("boom")

    receiver(Unheard, key="probe.identified", on_failure=seen.append)(failing)
    _fire_and_drain()

    first = seen[0]
    assert first.receiver_key == "probe.identified"
    assert first.event_name == "testapp.Unheard"
    assert first.attempt == 1
    assert first.delivery_id > 0
    assert first.event_id > 0
    assert "boom" in first.error


def test_it_reports_failed_then_dead_across_the_attempt_budget() -> None:
    # Both are worth recording and they are different things: "it failed again"
    # and "it will not be tried again".
    seen: list[DeliveryFailure] = []

    def failing(event: Unheard) -> None:
        raise RuntimeError("boom")

    receiver(Unheard, key="probe.budget", max_attempts=3, on_failure=seen.append)(failing)
    _fire_and_drain()

    assert [failure.status for failure in seen] == [
        DeliveryStatus.FAILED,
        DeliveryStatus.FAILED,
        DeliveryStatus.DEAD,
    ]
    assert [failure.attempt for failure in seen] == [1, 2, 3]


def test_a_raising_hook_is_swallowed_and_logged(caplog: pytest.LogCaptureFixture) -> None:
    # It is on the failure path. A failure path that fails leaves the operator
    # with a traceback about logging rather than about the delivery.
    def failing(event: Unheard) -> None:
        raise RuntimeError("boom")

    def broken(failure: DeliveryFailure) -> None:
        raise ValueError("the log is down")

    receiver(Unheard, key="probe.brokenhook", max_attempts=1, on_failure=broken)(failing)
    with caplog.at_level(logging.ERROR):
        _fire_and_drain()

    assert "on_failure hook" in caplog.text
    # And the delivery row is still correct, which is what the swallow buys.
    assert DeliveryRecord.objects.get(receiver_key="probe.brokenhook").status == DeliveryStatus.DEAD


def test_a_receiver_without_a_hook_is_unaffected() -> None:
    def failing(event: Unheard) -> None:
        raise RuntimeError("boom")

    receiver(Unheard, key="probe.nohook", max_attempts=1)(failing)
    _fire_and_drain()

    assert DeliveryRecord.objects.get(receiver_key="probe.nohook").status == DeliveryStatus.DEAD


def test_a_succeeding_receiver_never_calls_it() -> None:
    seen: list[DeliveryFailure] = []

    receiver(Unheard, key="probe.fine", on_failure=seen.append)(lambda event: None)
    _fire_and_drain()

    assert seen == []


def test_a_worker_that_lost_the_row_does_not_report_the_failure() -> None:
    """The hook follows the write, not the attempt.

    A lease can lapse while its worker is still mid-receiver. That worker's
    verdict lands nowhere, because the fence is conditioned on a claim it no
    longer holds, and whoever legitimately took the row will attempt the
    delivery and report its own outcome. Reporting here too would log one
    attempt twice and, for a delivery log, invent a failure that never happened
    to anybody.
    """
    from datetime import timedelta

    from django_domain_events.delivery.claim_batch import claim_batch
    from django_domain_events.delivery.deliver import _fail, _Fence

    seen: list[DeliveryFailure] = []
    receiver(Unheard, key="probe.lostrow", on_failure=seen.append)(lambda event: None)

    with transaction.atomic():
        fire(Unheard(value=1))
    claim_batch(worker_id="A", now=_now(), lease=timedelta(seconds=-1), limit=10)
    row = DeliveryRecord.objects.get(receiver_key="probe.lostrow")
    zombie = _Fence(row)

    claim_batch(worker_id="B", now=_now(), lease=timedelta(hours=1), limit=10)

    assert _fail(zombie, row, "boom", attempt=1) is None
    assert seen == [], "a worker that wrote nothing must report nothing"
