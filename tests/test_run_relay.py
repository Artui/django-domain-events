from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import connection, transaction

from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.run_relay import run_relay
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

# The loop is backend-agnostic; only the concurrency guard is not. Separating
# them is what keeps the whole relay testable on the default backend, the same
# way the clock and the sleep are arguments.
UNSAFE = {"allow_unsafe_concurrency": True}


def test_it_refuses_where_locks_cannot_be_skipped() -> None:
    """Two relays on a backend without skipped locks would hand the same row to
    two receivers on every pass."""
    if connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend supports skipped locks")
    with pytest.raises(RuntimeError, match="SKIP LOCKED"):
        run_relay(worker_id="w1", passes=1)


def test_it_runs_where_the_backend_supports_skipped_locks(
    order: OrderPlaced, record: list[str]
) -> None:
    if not connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend cannot skip locks")
    with transaction.atomic():
        fire(order)
    record.clear()
    assert run_relay(worker_id="w1", passes=1) == {DeliveryStatus.SUCCEEDED: 2}


def test_it_claims_and_delivers(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    record.clear()

    assert run_relay(worker_id="w1", passes=1, **UNSAFE) == {DeliveryStatus.SUCCEEDED: 2}
    assert not DeliveryRecord.objects.exclude(status=DeliveryStatus.SUCCEEDED).exists()


def test_an_idle_pass_sleeps_rather_than_spinning() -> None:
    """The sleep is an argument so the branch is reachable without elapsing real
    seconds. Every branch in the loop turns on time; a suite that had to wait
    for one would either be slow or never reach it."""
    slept: list[float] = []
    run_relay(worker_id="w1", passes=2, sleep=slept.append, **UNSAFE)
    assert len(slept) == 2


def test_a_pass_with_work_does_not_sleep(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    slept: list[float] = []
    run_relay(worker_id="w1", passes=1, sleep=slept.append, **UNSAFE)
    assert slept == []


def test_the_clock_is_an_argument(order: OrderPlaced, record: list[str]) -> None:
    """A row serving a backoff wait is invisible until its time comes, and that
    is asserted by moving the clock rather than by waiting."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(available_at=datetime.now(timezone.utc) + timedelta(hours=1))

    assert run_relay(worker_id="w1", passes=1, sleep=lambda _: None, **UNSAFE) == {}

    later = lambda: datetime.now(timezone.utc) + timedelta(hours=2)  # noqa: E731
    assert run_relay(worker_id="w1", passes=1, now=later, sleep=lambda _: None, **UNSAFE) == {
        DeliveryStatus.SUCCEEDED: 2
    }
