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
from django_domain_events.delivery.fire import call_receiver, run_to_completion
from django_domain_events.delivery.hand_back import hand_back
from django_domain_events.delivery.not_yet import NotYet
from django_domain_events.delivery.permanent_failure import PermanentFailure
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.utils import (
    OnDeferral,
    lane_claim_sizes,
    served_lanes,
)
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.scope.causation import caused_by
from django_domain_events.settings import get_around_delivery, get_task_backend, setting
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
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
            # INFO rather than WARNING: a redelivered copy and one the queue
            # held past its lease are what the take exists to turn away, and a
            # queue draining a lapsed backlog turns away thousands of them with
            # nothing wrong (test_a_stale_copy_turned_away_is_logged_at_info).
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

    # The receiver is resolved before the lease is extended, because it is what
    # says how long the lease should be. A registry lookup touches no rows, so
    # nothing is written before ownership has been re-established.
    receiver = registry.receiver_for_key(delivery.receiver_key)
    seconds = _lease_seconds(receiver)

    # A row the claim already leased for this receiver starts without the
    # extension, which would rewrite the lease to what it already is at the
    # cost of a non-HOT update. The status is the band's fourth condition: a
    # row that is not CLAIMED is extended by nobody
    # (test_an_unclaimed_row_passed_directly_writes_no_lease).
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
        # Terminal: no amount of waiting brings back a deleted receiver, and a
        # row retrying forever is indistinguishable from a broken one.
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
        # Terminal: it will not decode on the next attempt either, and one
        # undecodable row must not stop the other four thousand.
        outcome = fence.write(
            status=DeliveryStatus.DEAD,
            attempts=delivery.attempts + 1,
            last_error=f"{type(exc).__name__}: {exc}"[:2000],
            completed_at=timezone.now(),
        )
        # It counted an attempt and dead-lettered the row, so it is logged like
        # any other failed attempt, and only once the write landed
        # (test_a_dead_letter_for_an_undecodable_payload_this_worker_could_not_record_logs_nothing).
        # The receiver's ``on_failure`` is not told: it was never handed this
        # payload, and opting it into hearing about one is a separate decision.
        if outcome is not None:
            _log_failure(
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
        # NULL read as the event's recorded_at, as give_up_after reads it.
        due_at=delivery.due_at if delivery.due_at is not None else delivery.event.recorded_at,
        # What a hook labels its span or log line by
        # (test_a_durable_receiver_is_told_its_own_key).
        receiver_key=receiver.key,
    )
    # The AROUND_DELIVERY hook wraps the attempt from out here, outside the
    # transaction carrying the acknowledgement, and is driven by hand rather
    # than by a ``with`` statement: by the time ``__exit__`` runs, the outcome
    # has been recorded, so the hook can neither undo an acknowledgement nor
    # turn a recorded success into a failure. Recorded, not necessarily
    # committed: a delivery run inside the caller's own ``atomic()`` writes into
    # that transaction
    # (test_the_outcome_is_committed_before_exit_runs,
    # test_inside_a_callers_transaction_the_outcome_is_recorded_not_committed).
    around = _Around(context)
    try:
        # Entering is inside the try: a hook that cannot be entered fails the
        # attempt exactly as the receiver raising would, without running it
        # (test_entering_raising_fails_the_attempt_without_running_the_receiver),
        # and a NotYet or RetryAfter from it is read as a receiver's would be.
        around.enter()
        with (
            transaction.atomic(using=write_alias()),
            caused_by(delivery.event.pk, delivery.event.correlation_id),
        ):
            call_receiver(receiver.func, receiver.takes_context, event, context)
            acknowledged = timezone.now()
            outcome = fence.write(
                status=DeliveryStatus.SUCCEEDED,
                attempts=attempt,
                completed_at=acknowledged,
                succeeded_at=acknowledged,
            )
            if outcome is None:
                # Lost the row mid-flight. Roll the receiver's work back with
                # the acknowledgement it belongs to rather than leaving the two
                # disagreeing, and let whoever holds the claim deliver it.
                logger.warning(
                    "worker %s finished %s after its lease lapsed; the work was "
                    "rolled back and the row belongs to another worker. Raise "
                    "lease_seconds= on receiver %s if it runs this long",
                    worker_id,
                    delivery_id,
                    receiver.key,
                )
                transaction.set_rollback(True, using=write_alias())
    except Exception as exc:
        # The receiver's verdict is read here, at the one boundary every
        # execution site shares, and nowhere else. Both are ordinary failures
        # in every other respect: the attempt counts and the message is kept.
        # A deferral that does not count is the exception, and the guard is one
        # arc of two conjuncts: the type is held by
        # test_an_ordinary_exception_is_not_read_as_a_deferral (an exception
        # with no ``counts`` would raise AttributeError here), ``counts`` by
        # test_a_counting_retry_after_still_spends_its_attempt.
        #
        # Recorded while the hook is still open, so ``on_failure`` and the
        # failure's log line run inside it
        # (test_on_failure_and_the_failure_warning_run_inside_the_hook).
        around.raised = exc
        if isinstance(exc, NotYet):
            return _reschedule(fence, delivery, exc)
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
        # A KeyboardInterrupt or a task runner's timeout signal is not a
        # verdict on the delivery. The hook sees it and it goes on up; the row
        # stays CLAIMED until its lease lapses, as it did before the hook
        # (test_a_base_exception_reaches_exit_and_propagates).
        around.raised = exc
        raise
    finally:
        # In a finally, so the hook is closed on every path out, including a
        # failure to record the outcome: a relay thread lives for days, and a
        # hook left open leaves its log context on every later line
        # (test_the_hook_is_exited_even_when_recording_the_failure_raises).
        around.exit()
    return outcome


class _Around:
    """The ``AROUND_DELIVERY`` hook for one attempt, entered and exited by hand.

    ``raised`` is the exception the attempt ended with, which ``exit`` hands to
    ``__exit__`` as a ``with`` statement would, so a tracing span gets its error
    status (test_exit_sees_the_receivers_exception_and_cannot_suppress_it).
    """

    def __init__(self, context: DeliveryContext) -> None:
        self.context = context
        self.raised: BaseException | None = None
        # The entered manager's bound ``__exit__``, or None while nothing is open.
        self._exit: Callable[..., object] | None = None

    def enter(self) -> None:
        """Resolve the hook, build its context manager and enter it.

        Resolving and building are part of entering: a path that does not
        import, or a factory that returns no context manager, fails the attempt
        like a raising ``__enter__``
        (test_a_hook_that_cannot_be_entered_at_all_fails_the_attempt). The
        manager is kept only once ``__enter__`` has returned, so one whose
        ``__enter__`` raised is never exited, which is the ``with`` statement's
        rule too (test_entering_raising_fails_the_attempt_without_running_the_receiver).
        ``__exit__`` is looked up before ``__enter__`` is called, as the
        ``with`` statement looks it up, so an object with no ``__exit__`` is
        refused before anything was entered
        (test_a_manager_with_no_exit_is_refused_before_it_is_entered).
        """
        factory = get_around_delivery()
        if factory is None:
            return
        manager = factory(self.context)
        exit_ = manager.__exit__
        manager.__enter__()
        self._exit = exit_

    def exit(self) -> None:
        """Close the hook, if one was entered, after the outcome was recorded.

        ``__exit__``'s return value is ignored: the failure it might suppress
        has already been recorded
        (test_exit_sees_the_receivers_exception_and_cannot_suppress_it). An
        ``Exception`` from it is logged and swallowed, and never reaches
        ``_fail``, which would overwrite a recorded success and run the work
        again
        (test_exit_raising_after_a_success_leaves_the_acknowledgement_and_is_logged).
        A ``BaseException`` from it propagates, and the recorded outcome stands
        (test_a_base_exception_from_exit_propagates_and_the_success_stands).
        """
        # Both are dropped first, on every path, the no-hook one included. The
        # exception holds its traceback, which holds ``_deliver_one``'s frame,
        # which holds this object: kept here, that is a cycle only the garbage
        # collector frees, and every failed attempt's frames, with whatever its
        # receiver held in its locals, would outlive it
        # (test_a_failed_attempt_leaves_no_reference_cycle).
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
    """Whether this row is still owed a run at this moment.

    The status set and the backoff are separate tests, because each guard here
    is one branch arc and coverage cannot see a deleted member:
    ``test_a_settled_row_delivered_again_does_not_run_its_receiver_twice``,
    ``test_a_dead_row_is_not_run_or_resurrected``,
    ``test_an_orphaned_row_is_not_run`` and ``test_a_cancelled_row_is_not_run``
    hold one terminal status each;
    ``test_a_failed_row_not_yet_due_is_not_run`` holds the backoff;
    ``test_a_failed_row_that_is_due_still_runs`` holds its ``available_at``
    half against a check that refuses every FAILED row, and
    ``test_a_failed_row_claimed_early_on_purpose_still_runs`` its status half
    against one that reads ``available_at`` alone.

    PENDING is held to its ``available_at`` as FAILED is, because a PENDING row
    is the one a receiver declared with ``due=`` writes, with its
    ``available_at`` at the due time
    (``test_a_pending_row_not_yet_due_is_not_run`` holds PENDING's membership,
    which coverage cannot see). Every other PENDING row is available from the
    moment it was written. A CLAIMED row is the claimer's to run:
    ``deliver_pending(ignore_backoff=True)`` claims a FAILED row early on
    purpose, and ``drain_outbox(now=...)`` a scheduled one, and the claim is
    what makes that row CLAIMED.
    """
    if delivery.status in TERMINAL:
        return False
    return not (delivery.status in RETRYABLE and delivery.available_at > now)


def _lease_seconds(receiver: RegisteredReceiver | None) -> int:
    """The lease this receiver's delivery needs: its own, or ``LEASE_SECONDS``.

    A row whose receiver is gone gets the setting, which is what the claim of
    the default lane, where such rows drain, falls back to as well.
    """
    if receiver is None or receiver.lease_seconds is None:
        return setting("LEASE_SECONDS")
    return receiver.lease_seconds


def _take(delivery_id: int, claimed_by: str | None, claimed_at: str | None, worker_id: str) -> bool:
    """Move a row from the claim a task message carries to this worker.

    One conditional ``UPDATE``, never a read followed by a save: two copies of
    one message dequeued at once each read a row that matches, and only a write
    the database serialises can tell them apart. The loser's ``WHERE`` is
    re-evaluated after the winner commits and matches nothing
    (``test_two_copies_at_once_run_the_receiver_once``, on Postgres).

    Outside any transaction of ours, so it commits on its own before the
    receiver's transaction opens. A take that rolled back with the receiver
    would leave the row under the old claim, takeable by the next copy.

    Every condition is one arc, so each has a test that fails without it:
    ``status`` - ``test_a_message_for_a_released_row_does_not_take_it``;
    ``claimed_by`` - ``test_a_message_naming_another_worker_does_not_take_the_row``;
    ``claimed_at`` -
    ``test_a_message_from_an_earlier_claim_by_the_same_worker_does_not_take_the_row``.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    taken = DeliveryRecord.objects.filter(
        pk=delivery_id,
        status=DeliveryStatus.CLAIMED,
        claimed_by=claimed_by,
        claimed_at=None if claimed_at is None else parse_datetime(claimed_at),
    ).update(claimed_by=worker_id, claimed_at=timezone.now())
    return bool(taken)


class _Fence:
    """Optimistic ownership check for one delivery's writes.

    ``claimed_at`` moves on every claim, so the pair with ``claimed_by`` is a
    fencing token: a write conditioned on both lands only while this call still
    owns the row.
    """

    def __init__(self, delivery: Any, worker_id: str | None = None) -> None:
        self.pk = delivery.pk
        # The owner this call believes it is, not the owner the row currently
        # names. A worker handed an id from a batch it claimed minutes ago may
        # have lost the row since; conditioning on what it just read would make
        # the check agree with whoever took it.
        self.claimed_by = worker_id if worker_id is not None else delivery.claimed_by
        self.claimed_at = delivery.claimed_at

    def _owned(self) -> Any:
        from django_domain_events.models.delivery_record import DeliveryRecord

        return DeliveryRecord.objects.filter(
            pk=self.pk, claimed_by=self.claimed_by, claimed_at=self.claimed_at
        )

    def leased_for(self, delivery: Any, now: datetime, seconds: int) -> bool:
        """Whether the row as loaded is this call's, under the lease it needs.

        True when the row names the owner this call believes it is and its
        lease runs out between ``seconds`` and ``seconds`` times the headroom
        from ``now``: what the extension would write, give or take the
        headroom a claim stamps. The caller then skips the extension, which is
        a write to ``lease_expires_at``, a column ``dde_claimed_by_lease`` is
        keyed on, so never a HOT update.

        Read from the row already loaded, so it costs no query. The extension's
        compare-and-set checks ownership a moment later than this read does,
        but a check at any instant before the receiver runs proves nothing
        past that instant: what keeps the row this call's from then on is the
        lease. A lease at least ``seconds`` ahead on this clock is one no other
        claimer can take before then unless its clock runs that far ahead of
        this one, which is what the extension assumed too, since it wrote
        ``now`` plus the same ``seconds`` on this clock. A takeover past that is
        caught by the final fenced write, as one past a lapsed lease always
        was.

        ``now`` is the clock the caller reads, ``timezone.now()``, naive under
        ``USE_TZ = False`` as the column is. A lease written on an injected
        clock moved forward, or back by more than a tenth of the lease, lies
        outside the band, so the extension runs as it always did. Hence the
        upper bound as well as the lower one. A clock moved back by less
        leaves it inside, and the row starts under a lease still at least
        ``seconds`` ahead, which is all the extension would have written
        (``test_a_row_claimed_on_a_slightly_earlier_clock_starts_without_extending``).

        The caller checks the status, the band's fourth condition. Each other
        condition is in one arc, so each names its test in
        ``tests/delivery/test_lease_band.py``: the owner -
        ``test_a_row_another_worker_took_in_band_is_not_run`` and
        ``test_a_row_another_worker_took_in_band_is_not_enqueued``; a lease at
        all - ``test_a_claimed_row_with_no_lease_is_extended_and_delivered``;
        the lower bound - ``test_a_row_started_late_in_its_batch_is_extended``;
        the upper bound - ``test_a_row_claimed_on_a_later_clock_is_extended``.
        """
        if delivery.claimed_by != self.claimed_by or delivery.lease_expires_at is None:
            return False
        remaining = delivery.lease_expires_at - now
        return (
            timedelta(seconds=seconds) <= remaining <= timedelta(seconds=seconds * LEASE_HEADROOM)
        )

    def extend_lease(self, now: datetime, seconds: int | None = None) -> bool:
        """Push the lease out to cover this delivery alone.

        A batch claim stamps one expiry across every row it took, and the relay
        then delivers them one at a time; without this the lease is a budget for
        the whole batch and runs out partway through it.

        ``seconds`` is the receiver's own lease, ``LEASE_SECONDS`` when None.
        This is the only moment a lease can be sized to the work, because it is
        the last one before the receiver's transaction opens and stops
        publishing. A row the claim already leased for the receiver needs none
        (``leased_for``).
        """
        lease = timedelta(seconds=seconds if seconds is not None else setting("LEASE_SECONDS"))
        return bool(self._owned().update(lease_expires_at=now + lease))

    def write(self, **fields: Any) -> DeliveryStatus | None:
        """Record an outcome, or return ``None`` if the row is no longer ours."""
        if not self._owned().update(**fields):
            return None
        return fields["status"]


def dispatch_one(
    delivery_id: int, *, worker_id: str | None = None, on_deferral: OnDeferral | None = None
) -> DeliveryStatus | None:
    """Deliver this row, or hand it to the task backend if that is its site.

    ``on_deferral`` is told when the receiver deferred without counting and
    the deferral was recorded, so the caller can pause that lane. Only for a
    row run here: one handed to a task backend defers inside the task, in a
    process holding no batch to pause.

    An enqueued row stays CLAIMED under its lease and is not counted as an
    outcome: nothing has happened to it yet. If the enqueue is lost the lease
    lapses and the relay reclaims it, which is what makes handing work to a
    lossy queue safe.

    The message carries the claim the row was handed off under, and the task
    must take the row under that claim before it runs anything. That is what
    makes a queue that redelivers safe as well as one that loses: a second copy,
    or a copy that outlived its lease, finds the claim gone and does nothing.

    The lease is extended here for the same reason ``deliver_one`` extends it,
    and it matters more: the row now has to survive the queue's backlog as well
    as the receiver's own runtime, and this is the last moment before the relay
    stops looking at it. A row handed off soon after its claim that already holds
    the receiver's lease, within the claim's headroom, is handed off as it
    is; the task then finds the same lease after its take and, if the queue was
    quick, starts without extending it either.
    """
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

    # Looked up only for a receiver that asked for it, so a misconfigured
    # TASK_BACKEND cannot break receivers that never wanted one.
    backend = get_task_backend()
    if backend is None:
        raise ImproperlyConfigured(
            f"Receiver {receiver.key!r} declares site='task' but no TASK_BACKEND "
            f"is configured, so there is nothing to hand it to."
        )
    delivery = DeliveryRecord.objects.get(pk=delivery_id)
    fence = _Fence(delivery, worker_id)
    # The message carries the claim, so only a row this worker holds can be
    # handed off: an unclaimed row has no claim to carry, and the task taking it
    # would be refused. Each half has its own test, since the two are one arc:
    # test_an_unclaimed_row_is_not_enqueued and test_a_lost_row_is_not_enqueued.
    # A row the claim already leased for this receiver is handed off without the
    # extension, under the same band as in ``_deliver_one``, and the queue still
    # has at least the receiver's lease
    # (test_a_hand_off_right_after_the_claim_writes_no_lease).
    now = timezone.now()
    seconds = _lease_seconds(receiver)
    if delivery.status != DeliveryStatus.CLAIMED or not (
        fence.leased_for(delivery, now, seconds) or fence.extend_lease(now, seconds)
    ):
        logger.warning(
            "worker %s does not hold delivery %s, so it was not enqueued", worker_id, delivery_id
        )
        return None

    # As an ISO string because it has to survive whatever the queue serialises
    # with: django.tasks refuses arguments that are not JSON, and a Celery
    # message under the default serializer is JSON too.
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
    """Record a failed attempt, dead-lettering once the budget is spent.

    ``error_type`` names the exception for the failure log; None is a failure
    no exception raised, an event nobody registers.

    ``terminal`` dead-letters now, whatever is left of the budget: the receiver
    raised ``PermanentFailure``. ``requested_delay`` replaces the backoff curve
    with the receiver's own schedule: it raised ``RetryAfter``. Neither changes
    how the attempt is counted, so neither can keep a row alive past the budget
    it was fired with.

    The curve is the receiver's own where it declared one, read from the
    registration ``deliver_one`` resolved for this attempt rather than from the
    row: a curve changed by a deploy applies to deliveries already in flight.
    """
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
        _report_failure(
            row, status=status, attempt=attempts, error=truncated, error_type=error_type
        )
    return outcome


def _defer(
    fence: _Fence,
    row: Any,
    receiver: RegisteredReceiver,
    exc: RetryAfter,
    attempt: int,
    on_deferral: OnDeferral | None,
) -> DeliveryStatus | None:
    """Record a deferral that does not count, or the give-up that ends it.

    ``attempts`` is never written here, whatever the outcome, and that is the
    invariant the whole feature is: an attempt is counted by the outcome of an
    attempt - a success, an ordinary failure, a counting ``RetryAfter`` - and
    the claim never touches it. Both halves are held by
    ``test_a_deferral_that_does_not_count_leaves_the_attempts_and_the_budget_alone``,
    which runs the deferral twice on a budget of one, so an increment in
    either the claim or this function dead-letters the row; the give-up half
    by ``test_a_deferral_past_give_up_after_dead_letters_the_row``.
    ``on_failure`` is told the attempt number the receiver's context carried,
    which the next run is given again.

    With no ``give_up_after`` nothing would end the delivery, so the deferral
    is counted after all, with a warning once per receiver per process
    (``test_a_deferral_with_no_give_up_after_counts_and_says_so_once``).

    The bound is measured from ``due_at``, or the event's ``recorded_at`` where
    that is NULL, which is when a row nobody has reopened became owed. Each arm
    has a test that fails without it: NULL -
    ``test_a_deferral_past_give_up_after_dead_letters_the_row``; ``due_at`` -
    ``test_due_at_is_read_before_the_event_timestamp`` and the replay test
    ``test_a_replayed_old_event_is_not_dead_lettered_on_its_first_deferral``.
    Past the bound the row is dead-lettered as a spent budget is: ``DEAD``,
    ``completed_at``, ``on_failure``, and a ``last_error`` naming the bound.

    Otherwise the next attempt is scheduled between the requested delay and
    twice that, clamped to ``MAX_RECEIVER_RETRY_DELAY_SECONDS``: never earlier
    than the destination asked, and spread so that rows deferred together do
    not return together. The caller is told the lane and the delay asked for,
    not the jittered one, because a pause longer than asked holds back rows
    the destination is ready for - and only once the deferral is recorded,
    since a worker that lost the row has no say over its lane
    (``test_a_deferral_this_worker_could_not_record_pauses_nothing``). A
    caller with no callback - a direct ``deliver_one``, the eager attempt, a
    task - pauses nothing
    (``test_a_deferral_run_directly_is_recorded_and_pauses_nothing``).
    """
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
            _report_failure(
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
        _report_failure(
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
    """Move a row its receiver answered with ``NotYet`` to the time it named.

    ``available_at`` and ``due_at`` together, because the row is not owed
    until then: ``available_at`` alone would keep the relay off it, but a
    drain - which reads ``due_at`` to tell the schedule from the backoff -
    would take it straight back, and health would age an extended offer from
    the due time it no longer has
    (``test_the_row_is_not_claimed_again_before_the_time``,
    ``test_a_time_past_the_retry_ceiling_is_not_capped``).

    ``attempts`` is never written, as in ``_defer``, and neither is
    ``last_error`` or the failure hook: nothing failed
    (``test_an_early_run_moves_the_row_to_the_time_and_spends_no_attempt``,
    ``test_it_is_not_a_failure_so_on_failure_is_not_told``). PENDING rather
    than FAILED for the same reason, which is also what keeps a direct
    ``deliver_one`` off it until then, since the owed check holds PENDING to
    its ``available_at`` as it holds FAILED.
    """
    logger.info(
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


def _left_unfinished(delivery_id: int) -> bool:
    """Whether a row ``dispatch_one`` returned no outcome for is still waiting.

    CLAIMED is a row handed to a queue, which has not run it yet; PENDING is
    one a task ran inside the hand-off and rescheduled with ``NotYet``. Either
    way the pass is done with it, where a FAILED, SUCCEEDED or DEAD row says the
    task ran and recorded an outcome the pass could act on like its own.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    return DeliveryRecord.objects.filter(
        pk=delivery_id, status__in=[DeliveryStatus.CLAIMED, DeliveryStatus.PENDING]
    ).exists()


def _given_up(owed_since: datetime, now: datetime, bound: timedelta) -> bool:
    """Whether a row owed since ``owed_since`` has reached ``bound`` at ``now``.

    At exactly the bound it has: ``give_up_after=timedelta(days=1)`` means a
    day and no longer. Its own function so both sides of the comparison can be
    pinned, which a real clock cannot do
    (``test_the_bound_is_reached_at_exactly_give_up_after``).
    """
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
    """A receiver's requested delay, clamped to ``MAX_RECEIVER_RETRY_DELAY_SECONDS``.

    Its own ceiling rather than ``BACKOFF_CAP_SECONDS``, which bounds a curve.
    Clamping a legitimate one-hour ``Retry-After`` to a backoff cap would hammer
    a destination that asked to be left alone.

    The ceiling exists at all because a row parked in the future is still owed.
    ``prune_events`` deletes only settled events, so a delivery told to wait a
    month holds its event past the retention window for that month, and a
    destination answering with an absurd value would hold it indefinitely.

    Clamped with a warning rather than refused: the destination's number is
    advice about its own readiness, and the delivery is still worth attempting
    at the ceiling. The warning names both numbers so an operator can tell
    whether the ceiling or the destination is the one to question.
    """
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


def _report_failure(
    row: Any, *, status: DeliveryStatus, attempt: int, error: str, error_type: str | None
) -> None:
    """Log a failed attempt this worker recorded, then tell its receiver.

    Every caller reaches this only once the fence write has landed, which is
    the one condition both halves share: a worker that lost the row recorded
    nothing, and the one holding it now will attempt the delivery and report
    its own outcome. The check sits at each of the three call sites, and each
    is held by its own test: in ``_fail`` by
    ``test_a_worker_that_lost_the_row_logs_no_failure`` and
    ``test_a_worker_that_lost_the_row_does_not_report_the_failure``; at
    ``_defer``'s give-up by
    ``test_a_give_up_this_worker_could_not_record_tells_on_failure_nothing``;
    and at its counted deferral by
    ``test_a_deferral_this_worker_could_not_record_pauses_nothing``.

    One warning per attempt, whether or not the receiver declared
    ``on_failure``: the hook is opt-in, and without it a failing receiver - or
    a failover that ran one twice - left a trace in its row and nowhere an
    operator's logs would show it
    (``test_a_failed_attempt_is_logged_with_what_identifies_it``). The type
    and not the traceback, because ``last_error`` carries the message, and a
    receiver failing on every retry would otherwise write a stack each time.
    Not rate limited: one line per attempt is bounded by the attempt budget
    and the backoff, and a deferral that spends no attempt by
    ``give_up_after`` (``test_a_deferral_and_its_give_up_are_logged_too``).
    """
    _log_failure(row, status=status, attempt=attempt, error_type=error_type)
    _notify_failure(row, status=status, attempt=attempt, error=error)


def _log_failure(row: Any, *, status: DeliveryStatus, attempt: int, error_type: str | None) -> None:
    """The one warning line a failed attempt writes, in one shape everywhere.

    Apart from ``_report_failure`` because a payload that will not decode is
    logged without telling the receiver's hook
    (``test_a_payload_that_will_not_decode_is_logged_as_it_is_dead_lettered``).
    """
    logger.warning(
        "receiver %s failed delivery %s on attempt %s with %s; the row is %s",
        row.receiver_key,
        row.pk,
        attempt,
        error_type if error_type is not None else "no exception",
        status,
    )


def _notify_failure(row: Any, *, status: DeliveryStatus, attempt: int, error: str) -> None:
    """Tell the receiver its attempt failed, if it asked to be told.

    Called from here rather than from the delivery block above, and that is the
    whole point of the hook. A receiver runs inside the transaction carrying its
    acknowledgement, so everything it wrote is rolled back the moment it raises;
    it had no way to keep a record of its own failure. This runs after that
    rollback and after the row is written, so what the hook writes survives.

    Only when the row was actually claimed by this worker -- ``outcome`` is None
    when the lease lapsed and somebody else owns the delivery now, and telling a
    receiver about a failure another worker will re-attempt would log the same
    attempt twice.

    A raising hook is logged and swallowed. It is on the failure path, and a
    failure path that fails leaves the delivery row correct and the operator
    with a traceback about logging rather than about the delivery.
    """
    receiver = registry.receiver_for_key(row.receiver_key)
    if receiver is None or receiver.on_failure is None:
        return
    try:
        # Through the receivers' adapter, so an ``async def`` hook runs rather
        # than being built and dropped, and a cancelled or refused one is an
        # ``Exception`` the line below can swallow
        # (test_an_async_on_failure_hook_that_fails_is_logged_and_swallowed).
        run_to_completion(
            receiver.on_failure,
            DeliveryFailure(
                delivery_id=row.pk,
                event_id=row.event_id,
                event_name=row.event.name,
                receiver_key=row.receiver_key,
                attempt=attempt,
                status=status,
                error=error,
                target=row.target,
            ),
        )
    except Exception:
        logger.exception(
            "on_failure hook for receiver %s raised while recording delivery %s; "
            "the delivery row is correct and the hook's record is lost",
            row.receiver_key,
            row.pk,
        )


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
    # Two guards, each one branch arc over two conditions, so each names the
    # test that fails without either of them: each refusal, and each value
    # accepted alone, are asserted in
    # test_a_limit_and_a_batch_size_together_are_refused and
    # test_a_limit_with_lane_sizes_is_refused.
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
    # Lanes this call claims no more. A lane set aside by a deferral that did
    # not count, for the rest of the call rather than for the requested delay:
    # a pass has no clock to wait on, and with the backoff ignored - as
    # drain_outbox ignores it - nothing else would stop it claiming the
    # deferred row again, forever (test_a_drain_meeting_a_deferral_ends). And,
    # under a limit, a lane found drained, whose unspent share the others take.
    closed: set[str] = set()
    deferred: list[str] = []
    # What is left of ``limit``, re-read at every claim. None without one.
    remaining = limit
    # Rows this call is done with although the claim could still take them,
    # left out of its later claims, when it was told a ``now``. Two kinds, and both are rows the claim
    # would not take again on the real clock but would when told a later
    # ``now``:
    #
    # - A row a receiver answered with NotYet. The time it was moved to is
    #   ahead of the real clock, and a receiver deciding by that clock would
    #   answer the same way every time, spending nothing
    #   (test_a_drain_told_a_later_time_runs_a_rescheduled_row_once).
    #   Recognised by its outcome where it ran here: PENDING is what only a
    #   reschedule records.
    # - A row handed to a task backend, which returns no outcome. Read back
    #   once, and set aside if the hand-off left it CLAIMED - enqueued, under
    #   a lease extended on the real clock, which a later ``now`` reads as
    #   lapsed (test_a_drain_told_a_later_time_hands_a_task_row_to_the_queue_once)
    #   - or PENDING, a task that ran inside the hand-off and answered NotYet
    #   (test_a_task_answering_not_yet_under_a_drain_told_a_later_time_runs_once).
    #   A row the task failed is still claimed again, so a drain runs a failing
    #   task receiver to its dead letter as it runs one delivered in place
    #   (test_a_task_that_failed_inside_the_hand_off_is_retried_within_the_drain).
    set_aside: set[int] = set()
    while True:
        # Every lane is re-read at each rotation under None, as membership is
        # at each claim.
        rotation = [
            name for name in (registry.lanes() if lanes is None else lanes) if name not in closed
        ]
        claimed = False
        for turn, this_lane in enumerate(rotation):
            if remaining is None:
                size = size_of(this_lane)
            elif remaining == 0:
                # Spent: no claim of nothing for the lanes after it
                # (test_a_limit_below_the_number_of_lanes_serves_the_first_lanes).
                return counts
            else:
                size = math.ceil(remaining / (len(rotation) - turn))
            claimed_at = in_project_time(now) if now is not None else timezone.now()
            ids = claim_batch(
                worker_id=worker_id,
                now=claimed_at,
                # The lane's own lease with its headroom, so a row that starts
                # soon after the claim needs no extension; read per claim, as the
                # lane's membership is (test_a_uniform_lane_is_claimed_under_its_own_lease,
                # test_every_lane_is_claimed_under_its_own_lease).
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
                # Only where ``now`` was passed: on the real clock a rescheduled
                # row's time and a handed-off row's lease are both ahead of the
                # clock when written, so a later claim takes the row again only
                # once that time has really come. Nothing to exclude, and skipping the
                # read-back spares a query per task hand-off and keeps the claim's
                # NOT IN list empty. Held by
                # test_a_task_row_on_the_real_clock_is_not_read_back_or_excluded and
                # test_a_row_rescheduled_in_place_on_the_real_clock_is_not_excluded.
                if now is not None:
                    if outcome is DeliveryStatus.PENDING:
                        set_aside.add(delivery_id)
                    elif outcome is None and _left_unfinished(delivery_id):
                        # Read back only where no outcome came back, so a row run in
                        # place costs the pass no query. ``outcome is None`` changes
                        # no result - a row run in place is never left CLAIMED or
                        # PENDING without saying so - only the query count, which is
                        # what test_a_row_delivered_in_place_is_never_read_back holds.
                        set_aside.add(delivery_id)
                if outcome is not None:
                    counts[outcome] = counts.get(outcome, 0) + 1
                if deferred:
                    # One delivery defers at most once. Every row of the batch
                    # is in the lane that deferred, because it was claimed for
                    # that lane alone, so the rest goes back with no query to
                    # sort it (test_a_deferral_asks_nothing_about_the_rows_it_hands_back).
                    closed.add(deferred.pop())
                    hand_back(unstarted, worker_id=worker_id, claimed_at=claimed_at)
                    break
            if remaining is not None:
                # Counted as attempted, so a row handed to a task backend
                # spends the limit as it always did, and a row handed back
                # does not.
                remaining -= len(ids) - len(unstarted)
                if len(ids) < size:
                    closed.add(this_lane)
        if not claimed:
            return counts
