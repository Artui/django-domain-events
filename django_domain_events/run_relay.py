from __future__ import annotations

import itertools
import logging
import time as time_module
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from django.db import connections

from django_domain_events.claim_batch import claim_batch
from django_domain_events.deliver import dispatch_one
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.wake import wait_for_work
from django_domain_events.write_alias import write_alias

logger = logging.getLogger(__name__)


def run_relay(
    *,
    worker_id: str,
    passes: int | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time_module.sleep,
    wait: Callable[[float], bool] | None = None,
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
    connection = connections[write_alias()]
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
            outcome = _deliver_or_survive(delivery_id, worker_id)
            if outcome is not None:
                counts[outcome] = counts.get(outcome, 0) + 1
        if not ids:
            # Waits on a notification where the backend has one and sleeps where
            # it does not, so an event fired a moment ago is delivered in
            # milliseconds rather than at the next poll. The poll is still the
            # floor: a notification sent while nobody was listening is lost.
            _wait_or_survive(wait or (lambda t: wait_for_work(t, sleep=sleep)), poll, worker_id)
    return counts


def _wait_or_survive(wait: Callable[[float], bool], poll: float, worker_id: str) -> None:
    """Idle without letting a database blip take the daemon down.

    The wait reaches past Django's cursor to the driver, so a connection dropped
    during it raises the driver's own exception rather than a translated
    ``django.db.Error`` - which a supervisor written to catch the latter would
    miss. The relay spends nearly all its life in here, and the previous
    behaviour was a plain sleep, blind to the database; a failover surfaced at
    the next claim as a proper Django error, and it still will.
    """
    try:
        wait(poll)
    except Exception:
        logger.exception("relay %s could not wait for work", worker_id)


def _deliver_or_survive(delivery_id: int, worker_id: str) -> DeliveryStatus | None:
    """Deliver one row, and keep the daemon alive if it fails unexpectedly.

    ``dispatch_one`` handles a receiver raising and a payload that will not
    decode. Anything else - the event pruned out from under a claimed batch, the
    database going away mid-pass - would otherwise kill the relay and strand
    every row it had already claimed until their leases lapsed.
    """
    try:
        return dispatch_one(delivery_id, worker_id=worker_id)
    except Exception:
        logger.exception("relay %s could not deliver %s", worker_id, delivery_id)
        return None
