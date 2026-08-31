from __future__ import annotations

import itertools
import time as time_module
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from django.db import connection

from django_domain_events.claim_batch import claim_batch
from django_domain_events.deliver import deliver_one
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus


def run_relay(
    *,
    worker_id: str,
    passes: int | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time_module.sleep,
    allow_unsafe_concurrency: bool = False,
) -> dict[DeliveryStatus, int]:
    """Claim and deliver until ``passes`` is spent, or forever if it is None.

    The clock and the sleep are arguments rather than module calls so the loop
    is testable without waiting: every branch here turns on time, and a suite
    that had to elapse real seconds to reach one would either be slow or never
    reach it.

    Refuses to start where the database cannot express a skipped lock. Two
    workers there would hand the same row to two receivers on every pass, which
    at-least-once tolerates but nobody wants as a steady state.
    ``allow_unsafe_concurrency`` lifts that for a deployment running exactly one
    relay, which is a real shape in development; running two under it is the
    thing the guard exists to prevent.
    """
    if not (allow_unsafe_concurrency or connection.features.has_select_for_update_skip_locked):
        raise RuntimeError(
            f"{connection.vendor} cannot do SELECT ... FOR UPDATE SKIP LOCKED, so "
            f"a relay on it is not safe against a second copy of itself. Use "
            f"deliver_events --once, or drain_outbox() in tests."
        )

    lease = timedelta(seconds=setting("LEASE_SECONDS"))
    batch_size = setting("BATCH_SIZE")
    poll = setting("POLL_SECONDS")

    counts: dict[DeliveryStatus, int] = {}
    for _ in itertools.count() if passes is None else range(passes):
        ids = claim_batch(worker_id=worker_id, now=now(), lease=lease, limit=batch_size)
        for delivery_id in ids:
            outcome = deliver_one(delivery_id)
            counts[outcome] = counts.get(outcome, 0) + 1
        if not ids:
            sleep(poll)
    return counts
