from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from django.db import connection, transaction

from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.run_relay import run_relay
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import receiver_replaced
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


def test_an_idle_pass_waits_rather_than_spinning() -> None:
    """The idle wait is an argument so the branch is reachable without elapsing
    real seconds. ``wait`` rather than ``sleep``: on a backend that can notify,
    the loop blocks on the notification instead of sleeping, so injecting the
    sleep would control the loop on SQLite and not on Postgres."""
    waited: list[float] = []
    run_relay(worker_id="w1", passes=2, wait=lambda t: bool(waited.append(t)), **UNSAFE)
    assert len(waited) == 2


def test_a_pass_with_work_does_not_wait(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    waited: list[float] = []
    run_relay(worker_id="w1", passes=1, wait=lambda t: bool(waited.append(t)), **UNSAFE)
    assert waited == []


def test_the_clock_is_an_argument(order: OrderPlaced, record: list[str]) -> None:
    """A row serving a backoff wait is invisible until its time comes, and that
    is asserted by moving the clock rather than by waiting."""
    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(available_at=datetime.now(timezone.utc) + timedelta(hours=1))

    assert run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE) == {}

    later = lambda: datetime.now(timezone.utc) + timedelta(hours=2)  # noqa: E731
    assert run_relay(worker_id="w1", passes=1, now=later, wait=lambda _: False, **UNSAFE) == {
        DeliveryStatus.SUCCEEDED: 2
    }


def test_the_isolation_helper_swallows_what_deliver_one_does_not() -> None:
    """deliver_one guards the receiver and the decode, not the row fetch. A row
    that vanished between the claim and the delivery must not take the daemon
    with it."""
    from django_domain_events.run_relay import _deliver_or_survive

    assert _deliver_or_survive(999_999, "w1") is None


def test_a_lost_row_does_not_stop_the_pass(order: OrderPlaced, record: list[str]) -> None:
    """The relay counts outcomes and skips rows it lost, which must not end the
    batch: two events so a lost row lands mid-pass rather than at the end."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    def steal_everything_else(evt: OrderPlaced) -> None:
        DeliveryRecord.objects.exclude(receiver_key="testapp.durable_receiver").update(
            claimed_by="someone-else", claimed_at=datetime.now(timezone.utc)
        )

    with transaction.atomic():
        fire(order)
        fire(order)

    with receiver_replaced("testapp.durable_receiver", steal_everything_else):
        counts = run_relay(worker_id="w1", passes=1, wait=lambda _: False, **UNSAFE)

    # Deterministic regardless of how many receivers are registered: the first
    # row delivered succeeds and takes every other row away from this worker, so
    # the rest are lost mid-pass rather than at its end.
    lost = DeliveryRecord.objects.filter(claimed_by="someone-else").count()
    assert lost >= 2, "nothing was lost mid-pass, so the loop was never resumed"
    assert counts == {DeliveryStatus.SUCCEEDED: 2}
