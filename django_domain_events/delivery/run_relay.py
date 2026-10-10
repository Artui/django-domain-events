from __future__ import annotations

import itertools
import logging
import time as time_module
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from django.db import Error, connections

from django_domain_events.delivery.backoff import backoff
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.wake import wait_for_work
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus

logger = logging.getLogger(__name__)

# The longest the relay waits between claims that keep failing. Well inside
# Kubernetes' default 30-second grace period, because the wait is a plain sleep
# that a stop request does not cut short: this is also the longest a database
# outage can hold up a shutdown. Kubernetes' own restart backoff, which is what
# recovered a relay that crashed on the claim, climbs to five minutes.
_CLAIM_BACKOFF_CAP_SECONDS = 15.0


def run_relay(
    *,
    worker_id: str,
    passes: int | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time_module.sleep,
    wait: Callable[[float], bool] | None = None,
    stop: Callable[[], bool] = lambda: False,
    allow_unsafe_concurrency: bool = False,
) -> dict[DeliveryStatus, int]:
    """Claim and deliver until ``passes`` is spent, ``stop`` says so, or forever.

    The clock and the sleep are arguments rather than module calls so the loop
    is testable without waiting: every branch here turns on time, and a suite
    that had to elapse real seconds to reach one would either be slow or never
    reach it.

    ``stop`` is read before every claim and before every delivery. When it
    returns true the relay finishes the delivery in hand, hands back the rest
    of its claimed batch, and returns: the rows it gives back are claimable by
    any worker at once, rather than when their lease lapses. An idle relay
    reads it when its wait ends, so it stops within ``POLL_SECONDS``.
    ``deliver_events`` wires it to SIGTERM and SIGINT.

    The relay survives its database. A claim, a delivery or a wait that fails
    is logged and the loop goes on; after a database error the connection is
    closed so the next query opens a fresh one, and a failed claim is retried
    after a backoff that doubles from ``POLL_SECONDS`` up to 15 seconds.

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
    failed_claims = 0
    for _ in itertools.count() if passes is None else range(passes):
        if stop():
            break
        # One reading of the clock for the claim and for any hand-back: it is
        # the claim's ``claimed_at``, which is half of the token a hand-back is
        # conditioned on.
        claimed_at = now()
        try:
            ids = claim_batch(worker_id=worker_id, now=claimed_at, lease=lease, limit=batch_size)
        except Exception as exc:
            failed_claims += 1
            logger.exception("relay %s could not claim", worker_id)
            _close_after(exc, connection, worker_id)
            sleep(
                backoff(
                    failed_claims, base=poll, cap=_CLAIM_BACKOFF_CAP_SECONDS, jitter=1.0
                ).total_seconds()
            )
            continue
        failed_claims = 0
        for position, delivery_id in enumerate(ids):
            if stop():
                _hand_back_or_survive(ids[position:], worker_id, claimed_at, connection)
                return counts
            outcome = _deliver_or_survive(delivery_id, worker_id, connection)
            if outcome is not None:
                counts[outcome] = counts.get(outcome, 0) + 1
        if not ids:
            # Waits on a notification where the backend has one and sleeps where
            # it does not, so an event fired a moment ago is delivered in
            # milliseconds rather than at the next poll. The poll is still the
            # floor: a notification sent while nobody was listening is lost.
            _wait_or_survive(
                wait or (lambda t: wait_for_work(t, sleep=sleep)), poll, worker_id, connection
            )
    return counts


def _wait_or_survive(
    wait: Callable[[float], bool], poll: float, worker_id: str, connection: Any
) -> None:
    """Idle without letting a database blip take the daemon down.

    The wait reaches past Django's cursor to the driver, so a connection dropped
    during it can raise the driver's own exception rather than a translated
    ``django.db.Error`` - which a supervisor written to catch the latter would
    miss, and which is why both count as a database error here.

    This is where a failover is most likely to be found, since the relay spends
    nearly all its life in here, and it is found in autocommit. Django closes a
    connection that dies inside a transaction when the rollback fails, but
    nothing closes one that dies outside one: without the close below, every
    later claim fails with "the connection is lost" for as long as the process
    lives.
    """
    try:
        wait(poll)
    except Exception as exc:
        logger.exception("relay %s could not wait for work", worker_id)
        _close_after(exc, connection, worker_id)


def _deliver_or_survive(delivery_id: int, worker_id: str, connection: Any) -> DeliveryStatus | None:
    """Deliver one row, and keep the daemon alive if it fails unexpectedly.

    ``dispatch_one`` handles a receiver raising and a payload that will not
    decode. Anything else - the event pruned out from under a claimed batch, the
    database going away mid-pass - would otherwise kill the relay and strand
    every row it had already claimed until their leases lapsed.
    """
    try:
        return dispatch_one(delivery_id, worker_id=worker_id)
    except Exception as exc:
        logger.exception("relay %s could not deliver %s", worker_id, delivery_id)
        _close_after(exc, connection, worker_id)
        return None


def _hand_back_or_survive(
    delivery_ids: list[int], worker_id: str, claimed_at: datetime, connection: Any
) -> None:
    """Give back the unstarted rest of a batch on the way out.

    A failure is logged and swallowed: the process is stopping either way, and
    rows it could not give back are reclaimed when their lease lapses, exactly
    as a crashed worker's are. Raising here would turn a clean shutdown into a
    traceback without saving a single row.
    """
    try:
        given = hand_back(delivery_ids, worker_id=worker_id, claimed_at=claimed_at)
    except Exception as exc:
        logger.exception("relay %s could not hand back %d rows", worker_id, len(delivery_ids))
        _close_after(exc, connection, worker_id)
        return
    logger.info("relay %s stopping; handed back %d unstarted rows", worker_id, given)


def _close_after(exc: Exception, connection: Any, worker_id: str) -> None:
    """Close the write connection if ``exc`` came from the database.

    Django reconnects only a connection that is closed: ``ensure_connection``
    opens a new one when the old is None and otherwise trusts it, and its
    health check belongs to the request cycle a relay never enters. Nothing in
    a long-running process closes a dead one for it, so the relay does. After a
    database error the connection is not worth trusting, and a needless close
    costs one reconnect.

    A database error is Django's own or the driver's, because the wait reaches
    past Django to the driver. Each half has a test that fails without it:
    ``test_a_failed_claim_does_not_kill_the_relay`` (Django's) and
    ``test_a_raw_driver_error_while_idling_closes_the_connection`` (the
    driver's); ``test_an_error_that_is_not_the_databases_keeps_the_connection``
    holds the check against closing on everything, which a misconfiguration
    raising once per row would turn into a reconnect per row.

    Closing can raise in turn. Django has discarded the connection object by
    then regardless, so that is logged rather than allowed to end the relay.
    """
    if not isinstance(exc, (Error, connection.Database.Error)):
        return
    try:
        connection.close()
    except Exception:
        logger.exception("relay %s could not close its connection", worker_id)
