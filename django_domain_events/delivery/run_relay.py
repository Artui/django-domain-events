from __future__ import annotations

import itertools
import logging
import time as time_module
from collections.abc import Callable, Collection, Mapping
from datetime import datetime, timedelta
from typing import Any

from django.db import Error, connections
from django.utils import timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.backoff import backoff
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.utils import OnDeferral, lane_claim_sizes, served_lanes
from django_domain_events.delivery.wake import wait_for_work
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.operations.prune_events import prune_events
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
) -> dict[DeliveryStatus, int]:
    """Claim and deliver until ``passes`` is spent, ``stop`` says so, or forever."""
    connection = connections[write_alias()]
    if not (allow_unsafe_concurrency or connection.features.has_select_for_update_skip_locked):
        raise RuntimeError(
            f"{connection.vendor} cannot do SELECT ... FOR UPDATE SKIP LOCKED, so "
            f"a relay on it is not safe against a second copy of itself. Use "
            f"deliver_events --once, or drain_outbox() in tests."
        )
    lanes = served_lanes(lane)
    size_of = lane_claim_sizes(lanes, batch_size=batch_size, lane_batch_sizes=lane_batch_sizes)
    poll = setting("POLL_SECONDS")
    sweep = setting("RELAY_PRUNE")
    sweep_every = setting("RELAY_PRUNE_SECONDS")
    swept_at = monotonic()

    counts: dict[DeliveryStatus, int] = {}
    failed_claims = 0
    paused: dict[str, datetime] = {}
    deferred: list[tuple[str, float]] = []
    for _ in itertools.count() if passes is None else range(passes):
        claimed = False
        for this_lane in registry.lanes() if lanes is None else lanes:
            if stop():
                _forget_pauses_or_survive(paused, worker_id, connection)
                return counts
            claimed_at = in_project_time(now())
            until = paused.get(this_lane)
            if until is not None:
                if until > claimed_at:
                    continue
                del paused[this_lane]
                _forget_pauses_or_survive([this_lane], worker_id, connection)
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
                    _record_pause_or_survive(
                        deferred_lane, paused_at, paused[deferred_lane], worker_id, connection
                    )
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


def _record_pause_or_survive(
    lane: str, paused_at: datetime, until: datetime, worker_id: str, connection: Any
) -> None:
    """Write this relay's pause of ``lane`` where operators can see it."""
    from django_domain_events.models.lane_pause_record import LanePauseRecord

    try:
        LanePauseRecord.objects.update_or_create(
            lane=lane, worker_id=worker_id, defaults={"paused_at": paused_at, "until": until}
        )
        LanePauseRecord.objects.filter(until__lte=paused_at).delete()
    except Exception as exc:
        logger.exception("relay %s could not record the pause of lane %s", worker_id, lane)
        _close_after(exc, connection, worker_id)


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
