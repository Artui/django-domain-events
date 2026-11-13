from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest
from django.db import connections, transaction

from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import receiver_registered
from tests.testapp.events import OrderPlaced

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("skip_locked_backend"),
]

LEASE = timedelta(seconds=300)


def _claim(worker_id: str, sink: list[list[int]], barrier: threading.Barrier) -> None:
    """Claim from this thread's own connection."""
    try:
        barrier.wait(timeout=10)
        sink.append(
            claim_batch(worker_id=worker_id, now=datetime.now(timezone.utc), lease=LEASE, limit=50)
        )
    finally:
        connections.close_all()


def test_two_workers_never_claim_the_same_row(order: OrderPlaced, record: list[str]) -> None:
    """The guarantee 0.2.0 exists for, asserted against real contention."""
    with transaction.atomic():
        for _ in range(20):
            fire(order)
    owed = set(DeliveryRecord.objects.values_list("pk", flat=True))
    assert len(owed) == 40

    sink: list[list[int]] = []
    barrier = threading.Barrier(2)
    threads = [threading.Thread(target=_claim, args=(f"w{i}", sink, barrier)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert len(sink) == 2, "a claiming thread did not finish"
    first, second = sink
    assert not set(first) & set(second), "a row was claimed by both workers"
    assert set(first) | set(second) == owed, "a row was claimed by neither"
    assert DeliveryRecord.objects.filter(status=DeliveryStatus.CLAIMED).count() == len(owed)


def test_two_relays_racing_a_lapse_to_dead_tell_on_failure_once(
    order: OrderPlaced, record: list[str]
) -> None:
    """The row lock, not luck, keeps the second relay from dead-lettering it again."""
    seen: list[DeliveryFailure] = []
    dying = RegisteredReceiver(
        key="tests.dying",
        event_class=OrderPlaced,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=2,
        eager=False,
        site="relay",
        on_failure=seen.append,
    )
    with receiver_registered(dying):
        with transaction.atomic():
            fire(order)
        now = datetime.now(timezone.utc)
        DeliveryRecord.objects.filter(receiver_key="tests.dying").update(
            status=DeliveryStatus.CLAIMED,
            attempts=1,
            claimed_by="dead",
            claimed_at=now - timedelta(hours=1),
            lease_expires_at=now - timedelta(minutes=30),
        )
        DeliveryRecord.objects.exclude(receiver_key="tests.dying").delete()

        sink: list[list[int]] = []
        barrier = threading.Barrier(2)
        threads = [threading.Thread(target=_claim, args=(f"w{i}", sink, barrier)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

    assert sink == [[], []]
    assert DeliveryRecord.objects.get().status == DeliveryStatus.DEAD
    assert len(seen) == 1, "on_failure was told twice of one lapse"
