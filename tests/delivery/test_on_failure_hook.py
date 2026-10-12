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

    declared = registry.receiver_for_key("probe.lostrow")
    assert _fail(zombie, row, declared, "boom", attempt=1) is None
    assert seen == [], "a worker that wrote nothing must report nothing"


# --- The failure log ----------------------------------------------------------
#
# The hook is opt-in, so a receiver without one failed in silence: the row said
# so, and nothing an operator's log pipeline reads did. One warning per failed
# attempt, under the condition the hook is called on.

DELIVER_LOGGER = "django_domain_events.delivery.deliver"


def _failure_logs(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.name == DELIVER_LOGGER
        and r.levelno == logging.WARNING
        and " failed delivery " in r.getMessage()
    ]


def test_a_failed_attempt_is_logged_with_what_identifies_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failover that sent twice left no trace anywhere but the row. The line
    names the receiver, the delivery, the attempt, what the row is now and the
    exception's type - enough to find the row - and carries no traceback,
    because ``last_error`` already holds the message and a failing receiver
    retried every few seconds would otherwise fill the log with stacks."""

    def failing(event: Unheard) -> None:
        raise RuntimeError("the endpoint said no")

    receiver(Unheard, key="probe.logged", max_attempts=2)(failing)
    with caplog.at_level(logging.WARNING, logger=DELIVER_LOGGER):
        _fire_and_drain()

    row = DeliveryRecord.objects.get(receiver_key="probe.logged")
    logged = _failure_logs(caplog)
    assert [r.getMessage() for r in logged] == [
        f"receiver probe.logged failed delivery {row.pk} on attempt 1 with RuntimeError; "
        f"the row is failed",
        f"receiver probe.logged failed delivery {row.pk} on attempt 2 with RuntimeError; "
        f"the row is dead",
    ]
    assert all(r.exc_info is None for r in logged)


def test_a_deferral_and_its_give_up_are_logged_too(caplog: pytest.LogCaptureFixture) -> None:
    """A deferral that spends no attempt is still an attempt that did not
    succeed, and the give-up that ends one is the dead letter an operator most
    needs to hear about. Logged under the attempt number the receiver was given."""
    from datetime import timedelta

    from django_domain_events.delivery.retry_after import RetryAfter

    def deferring(event: Unheard) -> None:
        raise RetryAfter(1, counts=False)

    receiver(Unheard, key="probe.deferred", give_up_after=timedelta(hours=1))(deferring)
    with transaction.atomic():
        fire(Unheard(value=1))
    with caplog.at_level(logging.WARNING, logger=DELIVER_LOGGER):
        drain_outbox()
        EventRecord.objects.update(recorded_at=_now() - timedelta(hours=2))
        drain_outbox()

    row = DeliveryRecord.objects.get(receiver_key="probe.deferred")
    assert [r.getMessage() for r in _failure_logs(caplog)] == [
        f"receiver probe.deferred failed delivery {row.pk} on attempt 1 with RetryAfter; "
        f"the row is failed",
        f"receiver probe.deferred failed delivery {row.pk} on attempt 1 with RetryAfter; "
        f"the row is dead",
    ]


def test_a_worker_that_lost_the_row_logs_no_failure(caplog: pytest.LogCaptureFixture) -> None:
    """The log follows the write, as the hook does: the worker that holds the
    row now will attempt it and log its own outcome, and a line from the one
    that lost it would record an attempt nobody's row shows."""
    from datetime import timedelta

    from django_domain_events.delivery.claim_batch import claim_batch
    from django_domain_events.delivery.deliver import _fail, _Fence

    receiver(Unheard, key="probe.lostlog")(lambda event: None)
    with transaction.atomic():
        fire(Unheard(value=1))
    claim_batch(worker_id="A", now=_now(), lease=timedelta(seconds=-1), limit=10)
    row = DeliveryRecord.objects.get(receiver_key="probe.lostlog")
    zombie = _Fence(row)
    claim_batch(worker_id="B", now=_now(), lease=timedelta(hours=1), limit=10)

    with caplog.at_level(logging.WARNING, logger=DELIVER_LOGGER):
        assert _fail(zombie, row, registry.receiver_for_key("probe.lostlog"), "boom", 1) is None
    assert _failure_logs(caplog) == []


def test_a_failure_no_exception_raised_says_so(caplog: pytest.LogCaptureFixture) -> None:
    """An event nobody registers any more fails without the receiver running,
    so there is no exception type to name, and the line says that rather than
    inventing one."""
    from tests.conftest import event_deleted

    receiver(Unheard, key="probe.unregistered")(lambda event: None)
    with transaction.atomic():
        fire(Unheard(value=1))
    with event_deleted("testapp.Unheard"), caplog.at_level(logging.WARNING, logger=DELIVER_LOGGER):
        drain_outbox(limit=10)

    row = DeliveryRecord.objects.get(receiver_key="probe.unregistered")
    assert (
        f"receiver probe.unregistered failed delivery {row.pk} on attempt 1 with no exception"
        in (caplog.text)
    )
