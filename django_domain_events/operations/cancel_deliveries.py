from __future__ import annotations

from collections.abc import Iterable

from django.db import models, transaction
from django.utils import timezone

from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.utils import RETRYABLE


def cancel_deliveries(
    event_ids: Iterable[int],
    *,
    receiver_key: str | None = None,
    delivery_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """Withdraw the owed deliveries of these events, so they never run.

    The scheduled case is the one this exists for: an offer accepted before its
    expiry is due, a reminder for an order already paid. A receiver that finds
    nothing to do and returns is still the answer when the world moved on and
    nobody said so; this is the answer when the application knows ahead of
    time, and wants neither the run nor a success recorded for work that was
    never done.

    A delivery is withdrawn if it is **unclaimed and owed**: pending, which is
    a row waiting for its due time or for the relay, or failed, which is one
    waiting out its backoff - a scheduled row that came due and failed is
    still work, and "never runs" has to cover its retries. It becomes
    ``CANCELLED``, a settled status of its own, with ``completed_at`` set and
    nothing else touched. Not only scheduled rows: an immediate row the relay
    has not reached yet is withdrawn the same way, though the relay may well
    get there first.

    A **claimed** row is left alone. A worker holds it and may be running the
    receiver now, so settling it under the worker would record an outcome the
    receiver contradicts; the receiver re-checks current state when it runs,
    as it always did. It is counted, so the caller learns how many it could not
    stop. A settled row - succeeded, dead, orphaned or already cancelled -
    keeps its outcome: a dead letter withdrawn here would leave the
    dead-letter counts without anyone having read or requeued it.

    Narrowed to one receiver by ``receiver_key``, because one event usually
    drives an immediate reaction and a scheduled one, and only the scheduled
    one became moot. Narrowed to rows by ``delivery_ids``, which intersects
    with ``event_ids`` rather than widening it: a stale id list cannot reach
    another event's work.

    Returns ``{"cancelled": n, "in_flight": m}``: the rows withdrawn, and the
    claimed rows in the same scope that were left to run.

    Called inside a transaction on the database the package writes to, it
    joins it, so the cancel commits with the change that made the work moot or
    not at all. ``replay_events()`` reopens a cancelled row like any settled
    one; ``requeue_dead()`` does not touch it.

    Concurrency, on a backend with row locks. Each batch locks its events
    first, in primary key order, the lock ``replay_events``, ``requeue_dead``
    and ``prune_events`` take: a cancel issued while a replay of the event is
    open waits for it and then withdraws what it reopened
    (test_a_cancel_issued_during_a_replay_is_not_overtaken_by_it), and two
    callers locking overlapping events queue rather than deadlock, which the
    ordering in test_the_events_are_locked_before_their_rows_are_written holds.
    Against the prune the lock buys nothing observable: a prune deletes only
    an event with nothing owed and this writes only owed rows, so the two never
    contend for a delivery row
    (test_a_prune_during_a_cancel_loses_nothing_and_takes_the_event_after
    passes without it). A relay's
    claim takes no event lock, so the write itself carries the status
    predicate: a claim committed first leaves the row claimed and the write
    skips it (test_it_will_not_cancel_a_row_claimed_after_the_lock, and on two
    connections test_a_cancel_waits_for_a_claim_in_flight_and_leaves_the_row_
    claimed), and a write committed first leaves nothing for a claim to take.

    Each condition of the write is one branch arc, so each has a test that
    fails without it: pending by test_unclaimed_rows_are_settled_and_never_run;
    failed by test_a_failed_row_awaiting_its_retry_is_cancelled_too; the event
    scope by test_only_the_named_events_are_touched; the receiver by
    test_it_can_be_narrowed_to_one_receiver; the named rows by
    test_it_can_be_narrowed_to_named_rows, their intersection with the events
    by test_a_named_row_of_an_event_not_named_is_left_alone, and an empty list
    meaning no rows rather than every row by
    test_an_empty_row_list_cancels_nothing.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    alias = write_alias()
    chunk = setting("BATCH_SIZE")
    counts = {"cancelled": 0, "in_flight": 0}
    for locks, scope in _batches(sorted(set(event_ids)), delivery_ids, chunk):
        if receiver_key is not None:
            scope &= models.Q(receiver_key=receiver_key)
        with transaction.atomic(using=alias):
            # The events first, ordered as the prune orders them, and before
            # anything is read: the write below reads its rows in the same
            # statement, after any replay holding these events has committed.
            # Ascending, which is what two lockers need to agree on to avoid a
            # deadlock (test_the_events_are_locked_before_their_rows_are_written
            # holds the order, and that it precedes the write).
            list(
                EventRecord.objects.using(alias)
                .select_for_update()
                .filter(locks)
                .order_by("pk")
                .values_list("pk", flat=True)
            )
            now = timezone.now()
            counts["cancelled"] += (
                DeliveryRecord.objects.using(alias)
                .filter(scope, status__in=RETRYABLE)
                .update(status=DeliveryStatus.CANCELLED, completed_at=now)
            )
            counts["in_flight"] += (
                DeliveryRecord.objects.using(alias)
                .filter(scope, status=DeliveryStatus.CLAIMED)
                .count()
            )
    return counts


def _batches(
    events: list[int], delivery_ids: Iterable[int] | None, chunk: int
) -> list[tuple[models.Q, models.Q]]:
    """Each batch as the events to lock and the delivery rows it may write.

    By event when no rows are named, so the rows are chosen inside the
    transaction, after the lock. By named row otherwise: those are read first,
    a chunk at a time, keeping only the ones belonging to a named event, and
    their events are what gets locked. Either way no statement binds more than
    ``chunk`` ids, which is what keeps an admin select-all past SQLite's
    parameter ceiling from becoming an OperationalError.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    if delivery_ids is None:
        return [
            (models.Q(pk__in=part), models.Q(event__in=part))
            for part in (events[start : start + chunk] for start in range(0, len(events), chunk))
        ]
    named = set(events)
    wanted = sorted(set(delivery_ids))
    rows: list[int] = []
    for start in range(0, len(wanted), chunk):
        rows.extend(
            pk
            for pk, event in DeliveryRecord.objects.filter(
                pk__in=wanted[start : start + chunk]
            ).values_list("pk", "event_id")
            if event in named
        )
    return [
        (
            models.Q(pk__in=DeliveryRecord.objects.filter(pk__in=part).values("event_id")),
            models.Q(pk__in=part),
        )
        for part in (rows[start : start + chunk] for start in range(0, len(rows), chunk))
    ]
