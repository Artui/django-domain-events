from __future__ import annotations

import contextvars
import itertools
import logging
import time as time_module
from collections.abc import Callable, Collection, Mapping
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any

from django.db import Error, connections
from django.utils import timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.backoff import backoff
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.lane_rate import LaneRate
from django_domain_events.delivery.lane_workers import LaneWorkers, Result
from django_domain_events.delivery.utils import (
    OnDeferral,
    lane_claim_sizes,
    lane_pauses,
    lane_threads,
    served_lanes,
)
from django_domain_events.delivery.wake import wait_for_work
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.operations.prune_with_counts import prune_with_counts
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import DEFAULT_LANE
from django_domain_events.utils import in_project_time

logger = logging.getLogger(__name__)

# The longest the relay waits between claims that keep failing. Well inside
# Kubernetes' default 30-second grace period, because the wait is a plain sleep
# that a stop request does not cut short: this is also the longest a database
# outage can hold up a shutdown. Kubernetes' own restart backoff, which is what
# recovered a relay that crashed on the claim, climbs to five minutes.
_CLAIM_BACKOFF_CAP_SECONDS = 15.0

# How stale a threaded lane's view of the shared pauses may grow: it refills every
# latency/N, which would otherwise read LanePauseRecord dozens of times a second.
_PAUSE_READ_SECONDS = 1.0


def run_relay(
    *,
    worker_id: str,
    passes: int | None = None,
    now: Callable[[], datetime] = timezone.now,
    sleep: Callable[[float], None] = time_module.sleep,
    wait: Callable[[float], bool] | None = None,
    stop: Callable[[], bool] = lambda: False,
    allow_unsafe_concurrency: bool = False,
    lane: str | Collection[str] | None = DEFAULT_LANE,
    batch_size: int | None = None,
    lane_batch_sizes: Mapping[str, int] | None = None,
    monotonic: Callable[[], float] = time_module.monotonic,
    lane_concurrency: Mapping[str, int] | None = None,
    lane_rates: Mapping[str, float] | None = None,
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

    ``lane`` is the lane this process serves, or a collection of lanes: only
    the rows of receivers declared with that ``lane=``, read from the registry
    on every claim. The default lane is every row no named lane takes,
    including the rows of a receiver that has since been deleted, and it is
    what a relay serves unless told otherwise - the default relay staying out
    of a slow lane is the whole of the isolation. A ``str`` is one lane; a
    collection is served as its distinct members, and an empty one is refused.
    None serves every lane the registry declares, re-read at each pass, for a
    single relay in development. A lane no receiver is declared in is refused
    at start, because a relay serving it would claim nothing and look healthy
    doing it.

    A relay serving several lanes visits them in sorted order, one claim each,
    and claims each lane's batch immediately before delivering it, so no batch
    burns its lease while another lane delivers. ``stop`` is read before each
    lane's turn as well as before each row. One rotation is one pass, and the
    pass is idle - it may sweep, and it waits - only when every lane in it was
    empty or paused. Because the lanes take turns, a lane waits for the others
    at most one batch of each other unpaused lane it shares the relay with,
    times that lane's slowest delivery: the bound is set by their batch sizes
    and never by how much they are owed, so a burst in one lane delays another
    by what one of its batches takes. Each claim stamps its own lane's lease,
    ``None`` included. A failed claim ends the pass, since a database refusing
    one lane is refusing them all.

    Deliveries run one at a time within a relay unless ``lane_concurrency``
    gives its lane worker threads: ``{lane: N}`` delivers up to N rows of it at
    once, each thread on its own connection, so the process holds 1 + N. This
    thread then claims, pauses, sweeps and waits, and each claim takes only as
    many rows as there are idle workers, excluding the rows still in flight, so
    no row waits on its lease behind another. ``lane_rates`` caps a lane's
    starts at ``{lane: R}`` a second in this process, R a float, so 0.5 is one
    every two seconds; it counts every start, a task enqueue included, and runs
    the lane on threads even at one worker, claiming a row at a time. A rate is
    per process, so N relays in the lane start up to N x R a second. Each row
    runs in a copy of the context this call was made in, so a ``ContextVar``
    set before it, as ``RELAY_STARTUP`` may, is seen by every row and a value one
    row sets is seen by no other. A lane with threads or a rate must be the only
    lane this relay serves, and takes no ``batch_size`` or ``lane_batch_sizes``,
    since a prefetched row waiting its turn would spend its lease: both are
    refused at start. One worker and no rate is the loop described above.

    On a threaded lane a ``RetryAfter`` deferral pauses the lane as above: no
    row is started once this thread hears it, the claimed rows not yet started are handed back,
    and the rows already running record their own outcomes. The shared pauses
    are read at most once a second rather than once a pass, so a ``pause_lane``
    takes hold within a second plus the deliveries in flight. ``stop``, a spent
    ``passes`` or a worker's ``BaseException`` stops the claims, hands back the
    rows not yet started, and waits for those in flight until the last of their
    leases ends; a worker's ``BaseException`` is then re-raised here. One pass is
    one claim, or one wait when there is nothing to claim or no idle worker.

    ``batch_size`` sizes this relay's claims in place of ``BATCH_SIZE``, which
    also sizes prune batches and requeue chunks. ``lane_batch_sizes`` sizes the
    claims of the lanes it names in place of ``batch_size``, and is the lever
    on the bound above: a noisy lane given a small batch in a shared relay
    delays the others less. A size below one, or a lane in it this relay does
    not serve, is refused at start. Every row of a batch is
    claimed under one lease, and a row still waiting its turn when that lease
    lapses is taken by another relay, so a slow lane wants a batch it can work
    through inside the lease its claim stamps. That is the lane's own lease
    plus 10 percent where every durable receiver in it shares one no longer
    than ``LEASE_SECONDS``, and ``LEASE_SECONDS`` plus 10 percent otherwise: 66 seconds, not
    ``LEASE_SECONDS``, for a lane of receivers declaring ``lease_seconds=60``.
    A row that starts while its claimed lease is still its receiver's own
    starts without extending it, which saves a write per delivery, so a batch
    whose rows all start inside a tenth of the lease costs the fewest.

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

    ``now`` defaults to ``django.utils.timezone.now``, so its readings follow
    ``USE_TZ`` as the columns they are compared with do; an injected clock may
    answer aware under either setting, and is converted to the project's form
    (``test_a_relay_on_an_aware_clock_claims_and_delivers``).

    A receiver deferring with ``RetryAfter(seconds, counts=False)`` pauses its
    lane for ``seconds``, measured on ``now``: the unstarted rest of the batch,
    every row of which is in that lane, is handed back rather than attempted -
    each would cost a call to learn what the first already said - and is
    claimable by any worker at once. The relay writes the pause to
    ``LanePauseRecord`` - its lane, this ``worker_id``, when and until when -
    deleting in the same call every record, whoever wrote it, whose pause has
    passed, and deletes its own when the pause ends and when it returns.

    The pause is shared. Each pass starts by reading the unexpired records of
    the lanes it serves, in one query, and a lane any of them holds is not
    claimed: the relay goes on to the next lane it serves. So every relay in the
    lane honours the first deferral, an operator's ``pause_lane`` stops them
    all, and a ``resume_lane`` that deletes the records restarts them at their
    next pass, the relay that recorded the pause included. A lane is taken up
    again at its first turn at or after its latest pause ends, so within
    ``POLL_SECONDS`` of it for a relay with nothing else to do.

    Every one of those reads and writes is logged and survived if it fails. A
    relay whose write failed still holds its own pause until it ends on its
    clock, though no other relay sees it; a relay whose read failed goes on with
    the pauses it already holds, and drops one of its own only when a read that
    succeeded finds the record it wrote gone.
    """
    connection = connections[write_alias()]
    if not (allow_unsafe_concurrency or connection.features.has_select_for_update_skip_locked):
        raise RuntimeError(
            f"{connection.vendor} cannot do SELECT ... FOR UPDATE SKIP LOCKED, so "
            f"a relay on it is not safe against a second copy of itself. Use "
            f"deliver_events --once, or drain_outbox() in tests."
        )
    lanes = served_lanes(lane)
    size_of = lane_claim_sizes(lanes, batch_size=batch_size, lane_batch_sizes=lane_batch_sizes)
    threads = lane_threads(
        lanes,
        lane_concurrency=lane_concurrency,
        lane_rates=lane_rates,
        batch_size=batch_size,
        lane_batch_sizes=lane_batch_sizes,
    )
    if threads is not None:
        return _ThreadedLane(
            *threads,
            worker_id=worker_id,
            now=now,
            sleep=sleep,
            wait=wait or (lambda t: wait_for_work(t, sleep=sleep)),
            stop=stop,
            monotonic=monotonic,
            connection=connection,
        ).run(passes)
    poll = setting("POLL_SECONDS")
    sweep = setting("RELAY_PRUNE")
    sweep_every = setting("RELAY_PRUNE_SECONDS")
    swept_at = monotonic()

    counts: dict[DeliveryStatus, int] = {}
    failed_claims = 0
    paused: dict[str, datetime] = {}
    recorded: set[str] = set()
    deferred: list[tuple[str, float]] = []
    for _ in itertools.count() if passes is None else range(passes):
        claimed = False
        this_pass = registry.lanes() if lanes is None else lanes
        shared = _read_pauses_or_survive(this_pass, in_project_time(now()), worker_id, connection)
        if shared is not None and recorded - shared.keys():
            gone = recorded - shared.keys()
            recorded -= gone
            for each in gone:
                del paused[each]
            _forget_pauses_or_survive(gone, worker_id, connection)
        for this_lane in this_pass:
            if stop():
                _forget_pauses_or_survive(paused, worker_id, connection)
                return counts
            claimed_at = in_project_time(now())
            until = paused.get(this_lane)
            if until is not None:
                if until > claimed_at:
                    continue
                del paused[this_lane]
                recorded.discard(this_lane)
                _forget_pauses_or_survive([this_lane], worker_id, connection)
            held = (shared or {}).get(this_lane)
            if held is not None and held > claimed_at:
                continue
            try:
                ids = claim_batch(
                    worker_id=worker_id,
                    now=claimed_at,
                    lease=timedelta(
                        seconds=registry.claim_lease_seconds(
                            this_lane, default=setting("LEASE_SECONDS")
                        )
                    ),
                    limit=size_of(this_lane),
                    lane=this_lane,
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
                break
            failed_claims = 0
            claimed = claimed or bool(ids)
            unstarted = list(ids)
            while unstarted:
                if stop():
                    _hand_back_or_survive(unstarted, worker_id, claimed_at, connection)
                    _forget_pauses_or_survive(paused, worker_id, connection)
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
                if deferred:
                    deferred_lane, seconds = deferred.pop()
                    paused_at = in_project_time(now())
                    paused[deferred_lane] = paused_at + timedelta(seconds=seconds)
                    if _record_pause_or_survive(
                        deferred_lane, paused_at, paused[deferred_lane], worker_id, connection
                    ):
                        recorded.add(deferred_lane)
                    _set_aside_or_survive(
                        unstarted, deferred_lane, worker_id, claimed_at, connection
                    )
                    break
        else:
            if not claimed:
                if sweep and monotonic() - swept_at >= sweep_every:
                    swept_at = monotonic()
                    _sweep_or_survive(worker_id, connection, stop)
                _wait_or_survive(
                    wait or (lambda t: wait_for_work(t, sleep=sleep)),
                    poll,
                    worker_id,
                    connection,
                    sleep,
                )
    _forget_pauses_or_survive(paused, worker_id, connection)
    return counts


class _ThreadedLane:
    """One lane delivered by worker threads: this thread claims, pauses, sweeps and waits.

    Claims take only the free workers (one under a rate), so no row waits on its lease."""

    def __init__(
        self,
        lane: str,
        n: int,
        rate: float | None,
        *,
        worker_id: str,
        now: Callable[[], datetime],
        sleep: Callable[[float], None],
        wait: Callable[[float], bool],
        stop: Callable[[], bool],
        monotonic: Callable[[], float],
        connection: Any,
    ) -> None:
        self.lane = lane
        self.worker_id = worker_id
        self.now = now
        self.sleep = sleep
        self.wait = wait
        self.stop = stop
        self.monotonic = monotonic
        self.connection = connection
        self.poll = setting("POLL_SECONDS")
        self.lease = timedelta(
            seconds=registry.claim_lease_seconds(lane, default=setting("LEASE_SECONDS"))
        )
        self.gate = None if rate is None else LaneRate(rate, monotonic)
        self.counts: dict[DeliveryStatus, int] = {}
        self.held: dict[int, datetime] = {}
        self.paused_until: datetime | None = None
        self.recorded = False
        self.shared_until: datetime | None = None
        self.read_at: float | None = None
        self.sweep = setting("RELAY_PRUNE")
        self.sweep_every = setting("RELAY_PRUNE_SECONDS")
        self.swept_at = monotonic()
        self.failed_claims = 0
        self.workers = LaneWorkers(lane, n, worker_id, contextvars.copy_context())

    def run(self, passes: int | None) -> dict[DeliveryStatus, int]:
        """Feed the workers until ``passes`` is spent, ``stop`` says so or a worker raises."""
        try:
            for _ in itertools.count() if passes is None else range(passes):
                if self.stop() or self.workers.failed:
                    break
                self._pass()
        except BaseException:
            with suppress(BaseException):
                self.workers.stop(time_module.monotonic())
            raise
        self.workers.stop(self._deadline())
        self._collect(self.workers.results())
        if self.recorded:
            self._forget()
        return self.counts

    def _pass(self) -> None:
        """One claim for the free workers, or one wait when there is nothing to claim."""
        self._collect(self.workers.results())
        slots = self.workers.idle_slots()
        if self._paused() or not slots:
            self._idle()
            return
        delay = 0.0 if self.gate is None else self.gate.wait_time()
        if delay > 0:
            self.sleep(min(delay, self.poll))
            return
        claimed_at = in_project_time(self.now())
        try:
            ids = claim_batch(
                worker_id=self.worker_id,
                now=claimed_at,
                lease=self.lease,
                limit=slots if self.gate is None else 1,
                lane=self.lane,
                exclude_ids=sorted(self.held),
            )
        except Exception as exc:
            self.failed_claims += 1
            logger.exception("relay %s could not claim", self.worker_id)
            _close_after(exc, self.connection, self.worker_id)
            self.sleep(
                backoff(
                    self.failed_claims, base=self.poll, cap=_CLAIM_BACKOFF_CAP_SECONDS, jitter=1.0
                ).total_seconds()
            )
            return
        self.failed_claims = 0
        if not ids:
            self._idle()
            return
        self._give(ids, claimed_at)

    def _give(self, ids: list[int], claimed_at: datetime) -> None:
        """Start each claimed row; a stop, a pause or a dead worker hands back the rest."""
        for index, delivery_id in enumerate(ids):
            self._collect(self.workers.results())
            if self.stop():
                _hand_back_or_survive(ids[index:], self.worker_id, claimed_at, self.connection)
                return
            if self._paused():
                _set_aside_or_survive(
                    ids[index:], self.lane, self.worker_id, claimed_at, self.connection
                )
                return
            try:
                self.workers.give(delivery_id)
            except RuntimeError:
                _hand_back_or_survive(ids[index:], self.worker_id, claimed_at, self.connection)
                return
            self.held[delivery_id] = claimed_at
            if self.gate is not None:
                self.gate.started()

    def _collect(self, results: list[Result]) -> None:
        """Count finished rows and pause the lane on a deferral a worker heard."""
        for delivery_id, outcome, deferral in results:
            self.held.pop(delivery_id, None)
            if outcome is not None:
                self.counts[outcome] = self.counts.get(outcome, 0) + 1
            if deferral is not None:
                paused_at = in_project_time(self.now())
                until = paused_at + timedelta(seconds=deferral[1])
                if self.paused_until is None or until > self.paused_until:
                    self.paused_until = until
                    self.recorded = (
                        _record_pause_or_survive(
                            self.lane, paused_at, until, self.worker_id, self.connection
                        )
                        or self.recorded
                    )

    def _paused(self) -> bool:
        """Whether this relay's pause or any relay's recorded one holds the lane now."""
        moment = in_project_time(self.now())
        if self.read_at is None or self.monotonic() - self.read_at > _PAUSE_READ_SECONDS:
            self.read_at = self.monotonic()
            shared = _read_pauses_or_survive([self.lane], moment, self.worker_id, self.connection)
            if shared is not None:
                self.shared_until = shared.get(self.lane)
                if self.recorded and self.lane not in shared:
                    self.paused_until = None
                    self._forget()
        held = (self.paused_until, self.shared_until)
        return any(until is not None and until > moment for until in held)

    def _forget(self) -> None:
        """Delete this relay's record of a pause that no longer holds."""
        self.recorded = False
        _forget_pauses_or_survive([self.lane], self.worker_id, self.connection)

    def _idle(self) -> None:
        """Wait for a worker to finish, or, with none busy, sweep and wait for work."""
        if self.held:
            self._collect(self.workers.results(timeout=self.poll))
            return
        if self.sweep and self.monotonic() - self.swept_at >= self.sweep_every:
            self.swept_at = self.monotonic()
            _sweep_or_survive(self.worker_id, self.connection, self.stop)
        _wait_or_survive(self.wait, self.poll, self.worker_id, self.connection, self.sleep)

    def _deadline(self) -> float:
        """The real monotonic moment the last in-flight row's lease ends."""
        if not self.held:
            return time_module.monotonic()
        remaining = max(self.held.values()) + self.lease - in_project_time(self.now())
        return time_module.monotonic() + max(0.0, remaining.total_seconds())


def _wait_or_survive(
    wait: Callable[[float], bool],
    poll: float,
    worker_id: str,
    connection: Any,
    sleep: Callable[[float], None],
) -> None:
    """Idle without letting a database blip take the daemon down."""
    try:
        wait(poll)
    except Exception as exc:
        logger.exception("relay %s could not wait for work", worker_id)
        _close_after(exc, connection, worker_id)
        sleep(poll)


def _sweep_or_survive(worker_id: str, connection: Any, stop: Callable[[], bool]) -> None:
    """Prune, and keep the daemon alive if the database refuses."""
    started = time_module.monotonic()
    try:
        pruned = prune_with_counts(stop=stop)
    except Exception as exc:
        logger.exception("relay %s could not prune", worker_id)
        _close_after(exc, connection, worker_id)
        return
    if pruned.events or pruned.deliveries:
        logger.info(
            "relay %s pruned %d events and %d delivery rows in %.3f s",
            worker_id,
            pruned.events,
            pruned.deliveries,
            time_module.monotonic() - started,
        )


def _deliver_or_survive(
    delivery_id: int, worker_id: str, connection: Any, on_deferral: OnDeferral | None = None
) -> DeliveryStatus | None:
    """Deliver one row, and keep the daemon alive if it fails unexpectedly."""
    try:
        return dispatch_one(delivery_id, worker_id=worker_id, on_deferral=on_deferral)
    except Exception as exc:
        logger.exception("relay %s could not deliver %s", worker_id, delivery_id)
        _close_after(exc, connection, worker_id)
        return None


def _set_aside_or_survive(
    unstarted: list[int], lane: str, worker_id: str, claimed_at: datetime, connection: Any
) -> None:
    """Hand back the unstarted rest of a batch whose lane just paused."""
    try:
        given = hand_back(unstarted, worker_id=worker_id, claimed_at=claimed_at)
    except Exception as exc:
        logger.exception(
            "relay %s could not hand back the rows of paused lane %s; abandoning the "
            "%d unstarted rows of its batch to their lease",
            worker_id,
            lane,
            len(unstarted),
        )
        _close_after(exc, connection, worker_id)
        return
    logger.info(
        "relay %s paused lane %s after a deferral; handed back %d unstarted rows",
        worker_id,
        lane,
        given,
    )


def _read_pauses_or_survive(
    lanes: Collection[str], moment: datetime, worker_id: str, connection: Any
) -> dict[str, datetime] | None:
    """The latest unexpired pause of each of ``lanes``, by any writer; None if unreadable."""
    try:
        return lane_pauses(lanes, moment)
    except Exception as exc:
        logger.exception("relay %s could not read the lane pauses", worker_id)
        _close_after(exc, connection, worker_id)
        return None


def _record_pause_or_survive(
    lane: str, paused_at: datetime, until: datetime, worker_id: str, connection: Any
) -> bool:
    """Write this relay's pause of ``lane`` for every relay to read; False if it failed."""
    from django_domain_events.models.lane_pause_record import LanePauseRecord

    try:
        LanePauseRecord.objects.update_or_create(
            lane=lane, worker_id=worker_id, defaults={"paused_at": paused_at, "until": until}
        )
    except Exception as exc:
        logger.exception("relay %s could not record the pause of lane %s", worker_id, lane)
        _close_after(exc, connection, worker_id)
        return False
    try:
        LanePauseRecord.objects.filter(until__lte=paused_at).delete()
    except Exception as exc:
        logger.exception("relay %s could not delete the pauses that have passed", worker_id)
        _close_after(exc, connection, worker_id)
    return True


def _forget_pauses_or_survive(lanes: Collection[str], worker_id: str, connection: Any) -> None:
    """Delete this relay's pause records for ``lanes``, which no longer hold."""
    if not lanes:
        return
    from django_domain_events.models.lane_pause_record import LanePauseRecord

    try:
        LanePauseRecord.objects.filter(worker_id=worker_id, lane__in=sorted(lanes)).delete()
    except Exception as exc:
        logger.exception("relay %s could not delete its pause records", worker_id)
        _close_after(exc, connection, worker_id)


def _hand_back_or_survive(
    delivery_ids: list[int], worker_id: str, claimed_at: datetime, connection: Any
) -> None:
    """Give back the unstarted rest of a batch on the way out."""
    try:
        given = hand_back(delivery_ids, worker_id=worker_id, claimed_at=claimed_at)
    except Exception as exc:
        logger.exception("relay %s could not hand back %d rows", worker_id, len(delivery_ids))
        _close_after(exc, connection, worker_id)
        return
    logger.info("relay %s stopping; handed back %d unstarted rows", worker_id, given)


def _close_after(exc: Exception, connection: Any, worker_id: str) -> None:
    """Close the write connection if ``exc`` came from the database."""
    if not isinstance(exc, (Error, connection.Database.Error)):
        return
    try:
        connection.close()
    except Exception:
        logger.exception("relay %s could not close its connection", worker_id)
