from __future__ import annotations

import logging
import math
import random
from collections.abc import Callable, Collection, Mapping
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.utils import timezone

from django_domain_events.declaration.registry import LEASE_HEADROOM, registry
from django_domain_events.delivery.backoff import backoff
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import call_receiver
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.moot import Moot
from django_domain_events.delivery.not_yet import NotYet
from django_domain_events.delivery.permanent_failure import PermanentFailure
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.utils import (
    OnDeferral,
    lane_claim_sizes,
    log_failure,
    report_failure,
    served_lanes,
)
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.scope.causation import caused_by
from django_domain_events.settings import get_around_delivery, get_task_backend, setting
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import (
    RETRYABLE,
    TERMINAL,
    decode_payload,
    in_project_time,
    parse_datetime,
)

logger = logging.getLogger(__name__)

# The receivers already warned about raising ``RetryAfter(counts=False)`` with
# no ``give_up_after``. Per process, because the warning is about a declaration
# and a twenty-thousand row burst should say so once, not twenty thousand times.
_WARNED_UNBOUNDED: set[str] = set()


def deliver_one(
    delivery_id: int,
    *,
    worker_id: str | None = None,
    claimed_by: str | None = None,
    claimed_at: str | None = None,
) -> DeliveryStatus | None:
    """Run one delivery and record its outcome. ``None`` means this call recorded none.

    The receiver's work and the acknowledgement commit together, so a receiver
    touching only this database is effectively once: the duplicate at-least-once
    owes you cannot be observed. Side effects outside the database are
    at-least-once, as promised.

    Every write here is a compare-and-set against the claim this call read, and
    the lease is extended to cover this one delivery before the receiver runs,
    unless the claim already leased it for this receiver.
    Both exist for the same reason: a claim can lapse while its worker is still
    alive, and a worker that has lost its row must not go on to write a verdict
    over whoever legitimately took it. Losing the row returns ``None`` rather
    than raising - it is an ordinary outcome of a lease expiring, not a fault.

    A row that is not owed is refused the same way, whoever asks: one already
    settled (succeeded, dead or orphaned), one that failed and is waiting out
    its backoff, or one whose due time has not come. Running any of them would
    repeat committed work, resurrect a dead letter past its budget, ignore a
    ``RetryAfter`` the destination sent, or do scheduled work early.

    A receiver answering ``NotYet`` has its row moved to the time it named,
    with no attempt spent, and this returns ``PENDING``: the one outcome that
    leaves the row owed and not failed.

    A receiver answering ``Moot`` has its work rolled back and its row settled
    ``CANCELLED`` with that attempt, ``last_error`` reading ``moot: <reason>``
    and no ``succeeded_at``; ``on_failure`` is not called. This returns
    ``CANCELLED``.

    ``claimed_by`` and ``claimed_at`` are the claim a task message carries, as
    the relay passed them to ``TaskBackend.enqueue``. Given them, this call
    first *takes* the row: one conditional update, committed before anything
    else runs, that succeeds only while the row is still CLAIMED under exactly
    that claim, and moves it to ``worker_id`` - or to an id minted for this one
    take. A second copy of the message, a copy the queue held past its lease,
    and a copy for a row that has since settled all fail the take and return
    ``None`` without running the receiver.

    A receiver deferring with ``RetryAfter(counts=False)`` is recorded here as
    anywhere, but pauses nothing: a pause belongs to the relay process that
    claimed the row's batch, and a call made directly holds no batch.
    """
    return _deliver_one(
        delivery_id,
        worker_id=worker_id,
        claimed_by=claimed_by,
        claimed_at=claimed_at,
        on_deferral=None,
    )


def _deliver_one(
    delivery_id: int,
    *,
    worker_id: str | None,
    claimed_by: str | None,
    claimed_at: str | None,
    on_deferral: OnDeferral | None,
) -> DeliveryStatus | None:
    """``deliver_one``, telling ``on_deferral`` of a deferral that did not count."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    if claimed_by is not None or claimed_at is not None:
        worker_id = worker_id if worker_id is not None else f"task-{uuid4().hex}"
        if not _take(delivery_id, claimed_by, claimed_at, worker_id):
            logger.info(
                "a task for delivery %s carried a claim the row no longer holds "
                "(%s at %s); it was not run. Another copy of the message took it, "
                "the queue held it past its lease, or the row has settled",
                delivery_id,
                claimed_by,
                claimed_at,
            )
            return None

    delivery = DeliveryRecord.objects.select_related("event").get(pk=delivery_id)
    now = timezone.now()
    if not _owed(delivery, now):
        logger.info(
            "delivery %s is %s and not owed now, so it was not run",
            delivery_id,
            delivery.status,
        )
        return None
    fence = _Fence(delivery, worker_id)

    receiver = registry.receiver_for_key(delivery.receiver_key)
    seconds = _lease_seconds(receiver)

    if (
        delivery.status == DeliveryStatus.CLAIMED
        and not fence.leased_for(delivery, now, seconds)
        and not fence.extend_lease(now, seconds)
    ):
        logger.warning(
            "worker %s lost delivery %s before running it; its lease had lapsed "
            "and another worker holds the row",
            worker_id,
            delivery_id,
        )
        return None

    if receiver is None:
        return fence.write(
            status=DeliveryStatus.ORPHANED,
            last_error=(
                f"No receiver is registered under {delivery.receiver_key!r}. It was "
                f"renamed, moved or deleted after this delivery was recorded."
            ),
            completed_at=now,
        )

    event_entry = registry.event_for_name(delivery.event.name)
    if event_entry is None:
        return _fail(
            fence,
            delivery,
            receiver,
            f"No event is registered under {delivery.event.name!r}, so its "
            f"payload cannot be rebuilt.",
        )

    try:
        event = decode_payload(
            event_entry.event_class, delivery.event.payload, delivery.event.version
        )
    except Exception as exc:
        outcome = fence.write(
            status=DeliveryStatus.DEAD,
            attempts=delivery.attempts + 1,
            last_error=f"{type(exc).__name__}: {exc}"[:2000],
            completed_at=timezone.now(),
        )
        if outcome is not None:
            log_failure(
                delivery,
                status=DeliveryStatus.DEAD,
                attempt=delivery.attempts + 1,
                error_type=type(exc).__name__,
            )
        return outcome

    attempt = delivery.attempts + 1
    context = DeliveryContext(
        event_id=delivery.event.pk,
        event_name=delivery.event.name,
        attempt=attempt,
        actor_key=delivery.event.actor_key,
        actor_label=delivery.event.actor_label,
        scope=delivery.event.scope,
        target=delivery.target,
        delivery_id=delivery.pk,
        due_at=delivery.due_at if delivery.due_at is not None else delivery.event.recorded_at,
        receiver_key=receiver.key,
        occurred_at=delivery.event.occurred_at,
        correlation_id=delivery.event.correlation_id,
    )
    around = _Around(context)
    committed = False

    def mark_committed() -> None:
        nonlocal committed
        committed = True

    try:
        around.enter()
        with (
            transaction.atomic(using=write_alias()),
            caused_by(delivery.event.pk, delivery.event.correlation_id),
        ):
            transaction.on_commit(mark_committed, using=write_alias())
            call_receiver(receiver.func, receiver.takes_context, event, context)
            acknowledged = timezone.now()
            outcome = fence.write(
                status=DeliveryStatus.SUCCEEDED,
                attempts=attempt,
                completed_at=acknowledged,
                succeeded_at=acknowledged,
            )
            if outcome is None:
                logger.warning(
                    "worker %s finished %s after its lease lapsed; the work was "
                    "rolled back and the row belongs to another worker. Raise "
                    "lease_seconds= on receiver %s if it runs this long",
                    worker_id,
                    delivery_id,
                    receiver.key,
                )
                transaction.set_rollback(True, using=write_alias())
            elif fence.lease_expires_at is not None and acknowledged > fence.lease_expires_at:
                logger.warning(
                    "worker %s finished delivery %s %.1f s past its lease; another relay could "
                    "have claimed it meanwhile. Raise lease_seconds= on receiver %s",
                    worker_id,
                    delivery_id,
                    (acknowledged - fence.lease_expires_at).total_seconds(),
                    receiver.key,
                )
    except Exception as exc:
        if committed:
            logger.exception(
                "an on-commit callback raised after delivery %s of receiver %s "
                "committed; the delivery succeeded and stands. Django skipped the "
                "callbacks registered after it in that transaction, including "
                "robust ones; register the receiver's own callbacks with "
                "robust=True",
                delivery_id,
                receiver.key,
            )
            return outcome
        around.raised = exc
        if isinstance(exc, NotYet):
            return _reschedule(fence, delivery, exc)
        if isinstance(exc, Moot):
            return _settle_moot(fence, delivery, exc, attempt)
        if isinstance(exc, RetryAfter) and not exc.counts:
            return _defer(fence, delivery, receiver, exc, attempt, on_deferral)
        return _fail(
            fence,
            delivery,
            receiver,
            f"{type(exc).__name__}: {exc}",
            attempt=attempt,
            terminal=isinstance(exc, PermanentFailure),
            requested_delay=exc.seconds if isinstance(exc, RetryAfter) else None,
            error_type=type(exc).__name__,
        )
    except BaseException as exc:
        around.raised = exc
        raise
    finally:
        around.exit()
    return outcome


class _Around:
    """The ``AROUND_DELIVERY`` hook for one attempt, entered and exited by hand."""

    def __init__(self, context: DeliveryContext) -> None:
        self.context = context
        self.raised: BaseException | None = None
        # The entered manager's bound ``__exit__``, or None while nothing is open.
        self._exit: Callable[..., object] | None = None

    def enter(self) -> None:
        """Resolve the hook, build its context manager and enter it."""
        factory = get_around_delivery()
        if factory is None:
            return
        manager = factory(self.context)
        exit_ = manager.__exit__
        manager.__enter__()
        self._exit = exit_

    def exit(self) -> None:
        """Close the hook, if one was entered, after the outcome was recorded."""
        exit_, self._exit = self._exit, None
        exc, self.raised = self.raised, None
        if exit_ is None:
            return
        try:
            if exc is None:
                exit_(None, None, None)
            else:
                exit_(type(exc), exc, exc.__traceback__)
        except Exception:
            logger.exception(
                "the AROUND_DELIVERY hook raised on exit from delivery %s of receiver %s; "
                "the delivery's outcome was already recorded and stands",
                self.context.delivery_id,
                self.context.receiver_key,
            )


def _owed(delivery: Any, now: datetime) -> bool:
    """Whether this row is still owed a run at this moment."""
    if delivery.status in TERMINAL:
        return False
    return not (delivery.status in RETRYABLE and delivery.available_at > now)


def _lease_seconds(receiver: RegisteredReceiver | None) -> int:
    """The lease this receiver's delivery needs: its own, or ``LEASE_SECONDS``."""
    if receiver is None or receiver.lease_seconds is None:
        return setting("LEASE_SECONDS")
    return receiver.lease_seconds


def _take(delivery_id: int, claimed_by: str | None, claimed_at: str | None, worker_id: str) -> bool:
    """Move a row from the claim a task message carries to this worker."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    taken = DeliveryRecord.objects.filter(
        pk=delivery_id,
        status=DeliveryStatus.CLAIMED,
        claimed_by=claimed_by,
        claimed_at=None if claimed_at is None else parse_datetime(claimed_at),
    ).update(claimed_by=worker_id, claimed_at=timezone.now())
    return bool(taken)


class _Fence:
    """Optimistic ownership check for one delivery's writes."""

    def __init__(self, delivery: Any, worker_id: str | None = None) -> None:
        self.pk = delivery.pk
        # The owner this call believes it is, not the one the row now names.
        self.claimed_by = worker_id if worker_id is not None else delivery.claimed_by
        self.claimed_at = delivery.claimed_at
        self.lease_expires_at = delivery.lease_expires_at

    def _owned(self) -> Any:
        from django_domain_events.models.delivery_record import DeliveryRecord

        return DeliveryRecord.objects.filter(
            pk=self.pk, claimed_by=self.claimed_by, claimed_at=self.claimed_at
        )

    def leased_for(self, delivery: Any, now: datetime, seconds: int) -> bool:
        """Whether the row as loaded is this call's, under the lease it needs."""
        if delivery.claimed_by != self.claimed_by or delivery.lease_expires_at is None:
            return False
        remaining = delivery.lease_expires_at - now
        return (
            timedelta(seconds=seconds) <= remaining <= timedelta(seconds=seconds * LEASE_HEADROOM)
        )

    def extend_lease(self, now: datetime, seconds: int | None = None) -> bool:
        """Push the lease out to cover this delivery alone."""
        lease = timedelta(seconds=seconds if seconds is not None else setting("LEASE_SECONDS"))
        extended = bool(self._owned().update(lease_expires_at=now + lease))
        if extended:
            self.lease_expires_at = now + lease
        return extended

    def write(self, **fields: Any) -> DeliveryStatus | None:
        """Record an outcome, or return ``None`` if the row is no longer ours."""
        if not self._owned().update(**fields):
            return None
        return fields["status"]


def dispatch_one(
    delivery_id: int, *, worker_id: str | None = None, on_deferral: OnDeferral | None = None
) -> DeliveryStatus | None:
    """Deliver this row, or hand it to the task backend if that is its site."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    receiver_key = (
        DeliveryRecord.objects.filter(pk=delivery_id).values_list("receiver_key", flat=True).first()
    )
    receiver = registry.receiver_for_key(receiver_key) if receiver_key else None
    if receiver is None or receiver.site != "task":
        return _deliver_one(
            delivery_id,
            worker_id=worker_id,
            claimed_by=None,
            claimed_at=None,
            on_deferral=on_deferral,
        )

    backend = get_task_backend()
    if backend is None:
        raise ImproperlyConfigured(
            f"Receiver {receiver.key!r} declares site='task' but no TASK_BACKEND "
            f"is configured, so there is nothing to hand it to."
        )
    delivery = DeliveryRecord.objects.get(pk=delivery_id)
    fence = _Fence(delivery, worker_id)
    now = timezone.now()
    seconds = _lease_seconds(receiver)
    if delivery.status != DeliveryStatus.CLAIMED or not (
        fence.leased_for(delivery, now, seconds) or fence.extend_lease(now, seconds)
    ):
        logger.warning(
            "worker %s does not hold delivery %s, so it was not enqueued", worker_id, delivery_id
        )
        return None

    backend.enqueue(
        delivery_id, claimed_by=fence.claimed_by, claimed_at=fence.claimed_at.isoformat()
    )
    return None


def _fail(
    fence: _Fence,
    row: Any,
    receiver: RegisteredReceiver,
    message: str,
    attempt: int | None = None,
    *,
    terminal: bool = False,
    requested_delay: float | None = None,
    error_type: str | None = None,
) -> DeliveryStatus | None:
    """Record a failed attempt, dead-lettering once the budget is spent."""
    now = timezone.now()
    attempts = attempt if attempt is not None else row.attempts + 1
    exhausted = terminal or attempts >= row.max_attempts
    status = DeliveryStatus.DEAD if exhausted else DeliveryStatus.FAILED
    truncated = message[:2000]
    wait = (
        backoff(
            attempts,
            base=_or_setting(receiver.backoff_base_seconds, "BACKOFF_BASE_SECONDS"),
            cap=_or_setting(receiver.backoff_cap_seconds, "BACKOFF_CAP_SECONDS"),
            jitter=random.random(),
        )
        if requested_delay is None
        else _capped(row, requested_delay)
    )
    outcome = fence.write(
        status=status,
        attempts=attempts,
        last_error=truncated,
        completed_at=now if exhausted else None,
        available_at=now + wait,
    )
    if outcome is not None:
        report_failure(row, status=status, attempt=attempts, error=truncated, error_type=error_type)
    return outcome


def _defer(
    fence: _Fence,
    row: Any,
    receiver: RegisteredReceiver,
    exc: RetryAfter,
    attempt: int,
    on_deferral: OnDeferral | None,
) -> DeliveryStatus | None:
    """Record a deferral that does not count, or the give-up that ends it."""
    message = f"{type(exc).__name__}: {exc}"
    if receiver.give_up_after is None:
        _warn_unbounded(receiver.key)
        return _fail(
            fence,
            row,
            receiver,
            message,
            attempt=attempt,
            requested_delay=exc.seconds,
            error_type=type(exc).__name__,
        )

    now = timezone.now()
    owed_since = row.due_at if row.due_at is not None else row.event.recorded_at
    if _given_up(owed_since, now, receiver.give_up_after):
        truncated = (
            f"Gave up: owed since {owed_since.isoformat()}, past "
            f"give_up_after={receiver.give_up_after}, and still deferred. {message}"
        )[:2000]
        outcome = fence.write(status=DeliveryStatus.DEAD, last_error=truncated, completed_at=now)
        if outcome is not None:
            report_failure(
                row,
                status=DeliveryStatus.DEAD,
                attempt=attempt,
                error=truncated,
                error_type=type(exc).__name__,
            )
        return outcome

    requested = _capped(row, exc.seconds)
    ceiling = timedelta(seconds=setting("MAX_RECEIVER_RETRY_DELAY_SECONDS"))
    truncated = message[:2000]
    outcome = fence.write(
        status=DeliveryStatus.FAILED,
        last_error=truncated,
        completed_at=None,
        available_at=now + min(requested * (1 + random.random()), ceiling),
    )
    if outcome is not None:
        report_failure(
            row,
            status=DeliveryStatus.FAILED,
            attempt=attempt,
            error=truncated,
            error_type=type(exc).__name__,
        )
        if on_deferral is not None:
            on_deferral(receiver.lane, requested.total_seconds())
    return outcome


def _reschedule(fence: _Fence, row: Any, exc: NotYet) -> DeliveryStatus | None:
    """Move a row its receiver answered with ``NotYet`` to the time it named."""
    logger.debug(
        "receiver %s answered delivery %s with NotYet; it is due again at %s",
        row.receiver_key,
        row.pk,
        exc.until.isoformat(),
    )
    return fence.write(
        status=DeliveryStatus.PENDING,
        available_at=exc.until,
        due_at=exc.until,
        completed_at=None,
    )


def _settle_moot(fence: _Fence, row: Any, exc: Moot, attempt: int) -> DeliveryStatus | None:
    """Settle a row its receiver found moot as CANCELLED: neither a success nor a failure."""
    logger.debug(
        "receiver %s found delivery %s moot on attempt %s: %s",
        row.receiver_key,
        row.pk,
        attempt,
        exc.reason,
    )
    return fence.write(
        status=DeliveryStatus.CANCELLED,
        attempts=attempt,
        last_error=f"moot: {exc.reason}"[:2000],
        completed_at=timezone.now(),
    )


def _left_unfinished(delivery_id: int) -> bool:
    """Whether a row ``dispatch_one`` returned no outcome for is still waiting."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    return DeliveryRecord.objects.filter(
        pk=delivery_id, status__in=[DeliveryStatus.CLAIMED, DeliveryStatus.PENDING]
    ).exists()


def _given_up(owed_since: datetime, now: datetime, bound: timedelta) -> bool:
    """Whether a row owed since ``owed_since`` has reached ``bound`` at ``now``."""
    return now - owed_since >= bound


def _warn_unbounded(receiver_key: str) -> None:
    """Say once per process that a receiver defers with nothing to end it."""
    if receiver_key in _WARNED_UNBOUNDED:
        return
    _WARNED_UNBOUNDED.add(receiver_key)
    logger.warning(
        "receiver %s raised RetryAfter(counts=False) but declares no give_up_after, "
        "so nothing would ever end its deliveries; the deferral was counted against "
        "max_attempts instead. Declare give_up_after= on the receiver. Said once per "
        "process.",
        receiver_key,
    )


def _or_setting(declared: float | None, name: str) -> float:
    """A receiver's declared value, or the setting it overrides when it has none."""
    return setting(name) if declared is None else declared


def _capped(row: Any, seconds: float) -> timedelta:
    """A receiver's requested delay, clamped to ``MAX_RECEIVER_RETRY_DELAY_SECONDS``."""
    ceiling = setting("MAX_RECEIVER_RETRY_DELAY_SECONDS")
    if seconds > ceiling:
        logger.warning(
            "receiver %s asked for delivery %s to be retried in %ss, past "
            "MAX_RECEIVER_RETRY_DELAY_SECONDS of %ss; retrying in %ss instead",
            row.receiver_key,
            row.pk,
            seconds,
            ceiling,
            ceiling,
        )
        seconds = ceiling
    return timedelta(seconds=seconds)


def deliver_pending(
    limit: int | None = None,
    *,
    worker_id: str = "deliver_pending",
    ignore_backoff: bool = False,
    lane: str | Collection[str] | None = None,
    batch_size: int | None = None,
    lane_batch_sizes: Mapping[str, int] | None = None,
    now: datetime | None = None,
) -> dict[DeliveryStatus, int]:
    """Claim and deliver what is owed, and report the outcome.

    ``limit=None`` means everything owed, claimed in batches until none is left;
    a number caps it at that many. Claims go through the same leased path the
    relay uses, so a pass here and a running relay do not hand the same row to
    two receivers - on a backend with row locking. SQLite has none, so two
    concurrent passes there can both take the same row.

    ``lane`` delivers only that lane's rows, as ``run_relay(lane=...)`` claims
    them; ``"default"`` is every row no named lane takes. A ``str`` is one
    lane, and a collection is served as its distinct members; an empty one, a
    member that is not a ``str`` and a member no receiver is declared in are
    refused, as the relay refuses them. None, the default here, is every lane
    the registry declares, which is what this function did before lanes
    existed and what ``drain_outbox`` promises. ``deliver_events --once``
    passes the default lane unless told otherwise, as the relay does.

    The lanes are visited as the relay visits them: in sorted order, one claim
    each, each claimed under its own lease immediately before it is delivered,
    and again in turn until a whole rotation claims nothing
    (``test_a_set_is_served_once_per_lane_in_sorted_order_until_a_rotation_claims_nothing``).
    So a burst in one lane delays another by one of its batches rather than by
    the burst (``test_a_burst_in_one_lane_does_not_hold_back_another_under_every_lane``),
    and ``None`` delivers across lanes in that rotation rather than in one
    ``available_at`` order. In a project that names no lane, None is the
    default lane alone and sends what it always sent
    (``test_with_no_named_lanes_every_lane_sends_what_it_always_sent``).

    ``batch_size`` sizes each claim in place of ``BATCH_SIZE``, and
    ``lane_batch_sizes`` sizes the claims of the lanes it names in place of
    ``batch_size``; a size below one, or a lane in it this call does not
    serve, is refused.

    ``limit`` caps the call's total and is shared between the lanes with work,
    so a cron running ``--once --limit 100`` over a lane with a burst and a
    lane without one does not spend every run on the burst. Each lane's claim
    takes its share of what is left: what is left divided by the lanes still
    open in this rotation, itself included, rounded up
    (``test_a_limit_is_shared_between_the_lanes_with_work``). A lane that
    claims fewer than its share is drained, and is not claimed again in the
    call; what it did not take falls to the lanes after it, and the call
    rotates again while some of the limit is left and some lane is open
    (``test_a_drained_lanes_share_goes_to_the_lanes_with_work``). So a lane
    with work gets at least ``limit // n`` of a call serving ``n`` lanes, and
    with one lane this is one claim of ``limit`` rows. A limit below the
    number of lanes gives one row each to the first lanes in sorted order
    (``test_a_limit_below_the_number_of_lanes_serves_the_first_lanes``). Rows
    handed back on a deferral do not count against it, since they were not
    delivered
    (``test_rows_handed_back_on_a_deferral_do_not_count_against_the_limit``).

    The sizes cannot be combined with ``limit``, which sizes each claim as its
    lane's share of it: accepting both would silently ignore the sizes
    (``test_a_limit_and_a_batch_size_together_are_refused``,
    ``test_a_limit_with_lane_sizes_is_refused``).

    A receiver deferring with ``RetryAfter(counts=False)`` sets its lane aside
    for the rest of the call: the unstarted rest of the batch, every row of
    which is in that lane, is handed back rather than attempted, and the lane
    is not claimed again, while every other lane goes on being delivered
    (``test_a_deferral_sets_its_lane_aside_and_the_pass_delivers_the_rest``).
    Unlike the relay's pause, this one is held for the call alone and writes
    no ``LanePauseRecord``.

    ``now`` is the moment every claim of the call is made at, in place of the
    clock: what is due, which backoff has run out and which lease has lapsed
    are all read against it. It is how ``drain_outbox(now=...)`` moves a test
    forward to a receiver's due time. Only the claims read it; the attempt
    itself runs and is recorded on the real clock, since a receiver's
    acknowledgement is a fact about when it ran.
    """
    if limit is not None and batch_size is not None:
        raise ValueError(
            f"limit={limit} sizes each claim as its lane's share of it, so "
            f"batch_size={batch_size} would be ignored. Pass one or the other."
        )
    if limit is not None and lane_batch_sizes is not None:
        raise ValueError(
            f"limit={limit} sizes each claim as its lane's share of it, so "
            f"lane_batch_sizes would be ignored. Pass one or the other."
        )
    lanes = served_lanes(lane)
    size_of = lane_claim_sizes(lanes, batch_size=batch_size, lane_batch_sizes=lane_batch_sizes)

    counts: dict[DeliveryStatus, int] = {}
    closed: set[str] = set()
    deferred: list[str] = []
    remaining = limit
    set_aside: set[int] = set()
    while True:
        rotation = [
            name for name in (registry.lanes() if lanes is None else lanes) if name not in closed
        ]
        claimed = False
        for turn, this_lane in enumerate(rotation):
            if remaining is None:
                size = size_of(this_lane)
            elif remaining == 0:
                return counts
            else:
                size = math.ceil(remaining / (len(rotation) - turn))
            claimed_at = in_project_time(now) if now is not None else timezone.now()
            ids = claim_batch(
                worker_id=worker_id,
                now=claimed_at,
                lease=timedelta(
                    seconds=registry.claim_lease_seconds(
                        this_lane, default=setting("LEASE_SECONDS")
                    )
                ),
                limit=size,
                ignore_backoff=ignore_backoff,
                lane=this_lane,
                exclude_ids=set_aside,
            )
            claimed = claimed or bool(ids)
            unstarted = list(ids)
            while unstarted:
                delivery_id = unstarted.pop(0)
                outcome = dispatch_one(
                    delivery_id,
                    worker_id=worker_id,
                    on_deferral=lambda deferred_lane, _seconds: deferred.append(deferred_lane),
                )
                if now is not None:
                    if outcome is DeliveryStatus.PENDING:
                        set_aside.add(delivery_id)
                    elif outcome is None and _left_unfinished(delivery_id):
                        # Read back only if no outcome came back: a row run in place costs no query.
                        set_aside.add(delivery_id)
                if outcome is not None:
                    counts[outcome] = counts.get(outcome, 0) + 1
                if deferred:
                    closed.add(deferred.pop())
                    hand_back(unstarted, worker_id=worker_id, claimed_at=claimed_at)
                    break
            if remaining is not None:
                remaining -= len(ids) - len(unstarted)
                if len(ids) < size:
                    closed.add(this_lane)
        if not claimed:
            return counts
