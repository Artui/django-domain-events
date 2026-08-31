from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest
from django.db import connection, connections, transaction

from django_domain_events.claim_batch import claim_batch
from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import OrderPlaced

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(
        not connection.features.has_select_for_update_skip_locked,
        reason="skipped locks are what this asserts, so it needs a backend with them",
    ),
]

LEASE = timedelta(seconds=300)


def _claim(worker_id: str, sink: list[list[int]], barrier: threading.Barrier) -> None:
    """Claim from this thread's own connection.

    A claim test sharing one connection proves nothing: SKIP LOCKED is about what
    a second session sees while the first holds a row lock, and two claims on one
    connection never contend.
    """
    try:
        barrier.wait(timeout=10)
        sink.append(
            claim_batch(worker_id=worker_id, now=datetime.now(timezone.utc), lease=LEASE, limit=50)
        )
    finally:
        connections.close_all()


def test_two_workers_never_claim_the_same_row(order: OrderPlaced, record: list[str]) -> None:
    """The guarantee 0.2.0 exists for, asserted against real contention rather
    than inferred from the SQL."""
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
