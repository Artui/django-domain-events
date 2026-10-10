from __future__ import annotations

import itertools
import logging
import time as time_module
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from django.db import Error, connections

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.backoff import backoff
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import (
    OnDeferral,
    claim_size,
    dispatch_one,
    partition_by_lane,
)
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.wake import wait_for_work
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.operations.prune_events import prune_events
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import DEFAULT_LANE

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
    lane: str | None = DEFAULT_LANE,
    batch_size: int | None = None,
    monotonic: Callable[[], float] = time_module.monotonic,
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

    ``lane`` is the lane this process serves: only the rows of receivers
    declared with that ``lane=``, read from the registry on every claim. The
    default lane is every row no named lane takes, including the rows of a
    receiver that has since been deleted, and it is what a relay serves unless
    told otherwise - the default relay staying out of a slow lane is the whole
    of the isolation. None serves every lane, for a single relay in development.
    A lane no receiver is declared in is refused at start, because a relay
    serving it would claim nothing and look healthy doing it.

    Concurrency is the number of processes. Deliveries run one at a time within
    a relay, so a lane that must go faster runs more relays with the same
    ``lane``; nothing here limits the rate across them.

    ``batch_size`` sizes this relay's claims in place of ``BATCH_SIZE``, which
    also sizes prune batches and requeue chunks. Every row of a batch is
    claimed under one lease, and a row still waiting its turn when that lease
    lapses is taken by another relay, so a slow lane wants a batch it can work
    through inside ``LEASE_SECONDS``.

    A relay that finds nothing to claim also prunes, so an event declared to be
    deleted on consumption goes within minutes without a ``prune_events`` cron
    line. The sweep is ``prune_events()`` with its defaults, run from the idle
    branch at most once per ``RELAY_PRUNE_SECONDS``, the first one an interval
    after the relay starts rather than at its first idle pass: a relay that is
    restarted often, or run for a few passes, then never sweeps at all. Every
    relay sweeps whatever its lane. Nothing is lost or deleted twice, because
    the prune re-checks at the delete that an event is still due; the interval
    is what keeps N relays at N cheap queries per interval. ``RELAY_PRUNE`` set
    false turns it off for a project that schedules the prune itself. ``stop``
    is read between the sweep's batches, so a stop request waits for at most
    one batch of ``PRUNE_BATCH_ROWS`` rather than for a whole backlog.

    ``monotonic`` is the sweep's clock, apart from ``now`` because that one is a
    wall-clock reading a test sets to any value it likes for the claim, and a
    monotonic clock is not moved by an NTP step, which would silence or flood
    the sweep. It advances
    only when a sweep was attempted, and a failed one counts: it is not retried
    before the next interval.

    A receiver deferring with ``RetryAfter(seconds, counts=False)`` pauses its
    lane in this process for ``seconds``, measured on ``now``: the lane's
    unstarted rows in the batch are handed back rather than attempted - each
    would cost a call to learn what the first already said - and are claimable
    by any worker at once. While the pause lasts, a relay serving that lane
    claims nothing and idles, and one serving every lane leaves it out of its
    claims and goes on delivering the others, the rest of a mixed batch
    included. The pause is per process and shares nothing: every relay in the
    lane learns of the throttle from its own first deferral, at the cost of one
    call each. It ends at the first pass after ``seconds``, so within
    ``POLL_SECONDS`` of it.
    """
    connection = connections[write_alias()]
    if not (allow_unsafe_concurrency or connection.features.has_select_for_update_skip_locked):
        raise RuntimeError(
            f"{connection.vendor} cannot do SELECT ... FOR UPDATE SKIP LOCKED, so "
            f"a relay on it is not safe against a second copy of itself. Use "
            f"deliver_events --once, or drain_outbox() in tests."
        )
    registry.require_lane(lane)

    lease = timedelta(seconds=setting("LEASE_SECONDS"))
    batch_size = claim_size(batch_size)
    poll = setting("POLL_SECONDS")
    sweep = setting("RELAY_PRUNE")
    sweep_every = setting("RELAY_PRUNE_SECONDS")
    swept_at = monotonic()

    counts: dict[DeliveryStatus, int] = {}
    failed_claims = 0
    # Lane -> the moment its pause ends, on this process's clock. Nothing is
    # shared between relays: a pause is what this process learned from a
    # deferral it ran.
    paused: dict[str, datetime] = {}
    deferred: list[tuple[str, float]] = []
    for _ in itertools.count() if passes is None else range(passes):
        if stop():
            break
        # One reading of the clock for the claim and for any hand-back: it is
        # the claim's ``claimed_at``, which is half of the token a hand-back is
        # conditioned on.
        claimed_at = now()
        # A pause ends at its moment exactly, which
        # test_the_pause_ends_when_the_requested_delay_has_passed_on_the_injected_clock
        # holds from both sides.
        paused = {name: until for name, until in paused.items() if until > claimed_at}
        try:
            # The lane this relay serves, paused, is not even asked for and
            # falls through to the idle wait below
            # (test_a_relay_whose_lane_is_paused_does_not_query_it): the
            # exclusion would make the claim come back empty, but only after
            # walking past every row the lane is owed. ``lane in paused`` is
            # never true for None, so a relay serving every lane claims,
            # leaving the paused lanes out
            # (test_a_relay_serving_every_lane_keeps_claiming_the_other_lanes_while_one_is_paused).
            ids = (
                []
                if lane in paused
                else claim_batch(
                    worker_id=worker_id,
                    now=claimed_at,
                    lease=lease,
                    limit=batch_size,
                    lane=lane,
                    exclude_lanes=paused.keys(),
                )
            )
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
        unstarted = list(ids)
        while unstarted:
            if stop():
                _hand_back_or_survive(unstarted, worker_id, claimed_at, connection)
                return counts
            outcome = _deliver_or_survive(
                unstarted.pop(0),
                worker_id,
                connection,
                on_deferral=lambda deferred_lane, seconds: deferred.append(
                    (deferred_lane, seconds)
                ),
            )
            if outcome is not None:
                counts[outcome] = counts.get(outcome, 0) + 1
            while deferred:
                deferred_lane, seconds = deferred.pop()
                paused[deferred_lane] = now() + timedelta(seconds=seconds)
                unstarted = _set_aside_or_survive(
                    unstarted, deferred_lane, worker_id, claimed_at, connection
                )
        if not ids:
            # The guard is one branch arc, so each condition names the test that
            # fails without it. ``sweep``: test_a_relay_with_the_sweep_turned_
            # off_never_prunes. The interval: test_an_idle_relay_sweeps_once_per_
            # interval, which also holds ``>=`` against ``>`` and the stamp
            # moving on a sweep alone, and test_the_interval_is_the_setting.
            # That a pass which claimed work never sweeps is the enclosing
            # ``if not ids`` (test_a_pass_that_claimed_work_does_not_sweep), and
            # the stamp preceding the call rather than following a success is
            # test_a_sweep_that_fails_does_not_kill_the_relay_or_retry_early.
            if sweep and monotonic() - swept_at >= sweep_every:
                swept_at = monotonic()
                _sweep_or_survive(worker_id, connection, stop)
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


def _sweep_or_survive(worker_id: str, connection: Any, stop: Callable[[], bool]) -> None:
    """Prune, and keep the daemon alive if the database refuses.

    The caller has already moved the throttle's clock, so a sweep that fails is
    not retried before the next interval: a database that is down is already
    being retried by the claim, at a backoff, and a second client hammering it
    from the idle branch would be that much more load on the thing that needs
    quiet. Prune deletes in transactions of its own, so a failure leaves the
    batches it finished deleted and the rest due for the next sweep.
    """
    try:
        deleted = prune_events(stop=stop)
    except Exception as exc:
        logger.exception("relay %s could not prune", worker_id)
        _close_after(exc, connection, worker_id)
        return
    if deleted:
        logger.info("relay %s pruned %d events", worker_id, deleted)


def _deliver_or_survive(
    delivery_id: int, worker_id: str, connection: Any, on_deferral: OnDeferral | None = None
) -> DeliveryStatus | None:
    """Deliver one row, and keep the daemon alive if it fails unexpectedly.

    ``dispatch_one`` handles a receiver raising and a payload that will not
    decode. Anything else - the event pruned out from under a claimed batch, the
    database going away mid-pass - would otherwise kill the relay and strand
    every row it had already claimed until their leases lapsed.
    """
    try:
        return dispatch_one(delivery_id, worker_id=worker_id, on_deferral=on_deferral)
    except Exception as exc:
        logger.exception("relay %s could not deliver %s", worker_id, delivery_id)
        _close_after(exc, connection, worker_id)
        return None


def _set_aside_or_survive(
    unstarted: list[int], lane: str, worker_id: str, claimed_at: datetime, connection: Any
) -> list[int]:
    """Hand back the unstarted rows of a lane that just paused; return the rest.

    The rest is what the relay goes on delivering: the other lanes' rows of a
    batch claimed for every lane, or nothing, for a batch claimed for the
    paused lane alone.

    A failure abandons the whole rest of the batch rather than raising. Which
    rows were in the lane is then unknown, and attempting them could spend a
    call per throttled row, which is what the pause exists to avoid. What is
    abandoned is reclaimed when its lease lapses, exactly as a crashed
    worker's is (``test_a_set_aside_that_fails_abandons_the_batch``).
    """
    try:
        to_give_back, rest = partition_by_lane(unstarted, lane)
        given = hand_back(to_give_back, worker_id=worker_id, claimed_at=claimed_at)
    except Exception as exc:
        logger.exception(
            "relay %s could not hand back the rows of paused lane %s; abandoning the "
            "%d unstarted rows of its batch to their lease",
            worker_id,
            lane,
            len(unstarted),
        )
        _close_after(exc, connection, worker_id)
        return []
    logger.info(
        "relay %s paused lane %s after a deferral; handed back %d unstarted rows",
        worker_id,
        lane,
        given,
    )
    return rest


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
