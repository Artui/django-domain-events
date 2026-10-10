from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from django.db import models, transaction

from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.retention import Retention
from django_domain_events.utils import TERMINAL

_EVENTS_PER_BATCH = 500
"""The most events one batch names, whatever the row budget allows.

A batch is a list of ids bound as parameters, and a run of events with no
delivery rows would otherwise put ``PRUNE_BATCH_ROWS`` of them in one
statement - past the parameter limit of an older SQLite at the default."""


def prune_events(
    older_than: timedelta | None = None,
    *,
    now: datetime | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
    stop: Callable[[], bool] | None = None,
) -> int:
    """Delete the events that are due, and return how many went.

    An outbox without a prune story becomes the largest table in the database,
    and it becomes it quietly - which is why this ships rather than waiting for
    someone to notice. Nothing else deletes: an event's retention is carried
    out here or not at all.

    Due means one of three things, read off the row rather than the registry,
    so re-declaring or deleting an event class changes nothing for events
    already recorded:

    - **Consumed**, for an event recorded under ``Retention.SUCCEEDED`` (every
      delivery succeeded) or ``Retention.SETTLED`` (every delivery terminal).
      No window: it goes at the first prune after it is consumed, and an event
      with no delivery rows at all is consumed as soon as it commits.
    - **Past its own window**, for an event declared with a ``timedelta``, and
      settled.
    - **Past the ordinary window**, ``older_than`` or ``RETENTION_DAYS``, and
      settled, for every other event - including one recorded under
      ``Retention.SUCCEEDED`` that a dead letter kept, so the dead letter stays
      inspectable for the window rather than forever. ``older_than`` replaces
      the ordinary window only; an event with a window of its own keeps it.

    Settled means no delivery pending, failed or claimed: one still owed is
    work nobody recorded as lost, and deleting it would drop it. An event with
    no delivery rows - suppressed, or fired with no durable receivers - is
    settled by definition.

    Deletes in batches of at most ``batch_size`` rows (default
    ``PRUNE_BATCH_ROWS``), counting each event's delivery rows and the event
    row itself, so one transaction's lock and write volume stay bounded however
    the rows are spread. An event with more delivery rows than that has them
    deleted a batch at a time, each chunk in its own transaction, and goes
    with the last of them; until then it is an event with fewer deliveries,
    every one of them terminal. ``limit`` counts events.

    Every batch records each receiver's newest success among the rows it
    deletes, in the same transaction, which is what lets ``quiet_receivers()``
    still report a receiver whose events are all gone.

    Four queries, one per policy and window, none of which reads history:
    the ordinary window is a range on ``recorded_at``; each policy and the
    windows of their own read a partial index holding only their own live
    events, and check each against a partial index of unfinished delivery
    rows. With nothing due, the cost grows with how many events with a
    retention of their own are alive, and not with how much the tables have
    ever held. ``docs/retention.md`` has the measurement.
    """
    from django_domain_events.models.event_record import EventRecord

    size = batch_size if batch_size is not None else setting("PRUNE_BATCH_ROWS")
    # bool first: True is an int, and would read as a batch of one.
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ValueError(f"batch_size must be a positive number of rows, not {size!r}.")
    moment = now or datetime.now(timezone.utc)
    window = older_than if older_than is not None else timedelta(days=setting("RETENTION_DAYS"))

    unsettled = _deliveries_of_each().exclude(status__in=TERMINAL)
    unsucceeded = _deliveries_of_each().exclude(status=DeliveryStatus.SUCCEEDED)
    arms = [
        # One query per policy rather than their OR. An equality on the policy
        # column is one range of dde_consumed_by_policy, and with no OR the
        # NOT EXISTS becomes an anti-join probing dde_unfinished_by_event per
        # event. Under the OR, Postgres could do neither: it read the whole
        # retention index, and priced its per-event probes so high that it
        # read the entire delivery table into a hash instead, on every sweep
        # (test_each_kind_of_due_is_one_index_range holds the shape).
        #
        # An unknown policy value matches neither and falls through to the
        # ordinary window. Each conjunct is held by a test: the policy
        # equalities by test_an_ordinary_event_is_not_deleted_on_consumption;
        # the success-only test by test_a_fan_out_with_one_dead_row_is_kept_
        # until_every_delivery_succeeded and test_an_orphaned_delivery_keeps_
        # an_event_waiting_for_success; the settled test by
        # test_an_owed_delivery_keeps_it_under_either_policy[...-settled].
        EventRecord.objects.filter(delete_when=Retention.SUCCEEDED.value).exclude(
            models.Exists(unsucceeded)
        ),
        EventRecord.objects.filter(delete_when=Retention.SETTLED.value).exclude(
            models.Exists(unsettled)
        ),
        # ``retention_seconds IS NOT NULL`` is dde_own_window's own condition,
        # written out though the comparison implies it. No test holds it: the
        # Postgres the suite runs on proves the implication through the strict
        # arithmetic, and test_each_kind_of_due_is_one_index_range passes with
        # it deleted. It stays because the index serves only a query whose
        # WHERE implies its condition, and this spelling does not depend on how
        # far a given planner follows the arithmetic. The window is per row, so
        # it filters the index rather than bounding its range.
        EventRecord.objects.filter(retention_seconds__isnull=False)
        .alias(expires_at=models.F("recorded_at") + _Seconds(models.F("retention_seconds")))
        .filter(expires_at__lt=moment)
        .exclude(models.Exists(unsettled)),
        # retention_seconds IS NULL is what keeps an event's own longer window
        # from being cut short by this one (test_an_event_with_a_longer_window_
        # of_its_own_outlives_retention_days). delete_when is deliberately not
        # filtered: test_an_event_kept_by_a_dead_letter_still_goes_at_the_
        # ordinary_window.
        EventRecord.objects.filter(
            retention_seconds__isnull=True, recorded_at__lt=moment - window
        ).exclude(models.Exists(unsettled)),
    ]

    deleted = 0
    for due in arms:
        deleted += _prune(due, size, None if limit is None else limit - deleted, stop)
    return deleted


class _Seconds(models.Func):
    """A column of whole seconds as a duration, in the backend's own form.

    Django multiplies a duration by a number only where the backend has a
    native interval type, so ``F("retention_seconds") * timedelta(seconds=1)``
    raises on SQLite. Every backend without one stores a duration as an
    integer of microseconds, and Django's datetime arithmetic on those
    backends expects exactly that, so the default form is the multiplication
    by a million and Postgres gets its interval.
    """

    template = "(%(expressions)s * 1000000)"
    output_field = models.DurationField()

    def as_postgresql(self, compiler: Any, connection: Any, **extra: Any) -> Any:
        return self.as_sql(
            compiler, connection, template="(%(expressions)s * INTERVAL '1 second')", **extra
        )


def _deliveries_of_each() -> models.QuerySet[Any]:
    """The delivery rows of the event in the enclosing query."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    return DeliveryRecord.objects.filter(event=models.OuterRef("pk"))


def _prune(
    due: models.QuerySet[Any], size: int, limit: int | None, stop: Callable[[], bool] | None
) -> int:
    """Delete the events ``due`` selects, a batch of at most ``size`` rows at a
    time, until none is left, ``limit`` events have gone or ``stop`` says so.
    ``stop`` is read before each batch, so a request to stop waits for at most
    one batch (test_a_stop_ends_the_prune_between_batches)."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    deleted = 0
    while (limit is None or deleted < limit) and not (stop is not None and stop()):
        take = min(size, _EVENTS_PER_BATCH)
        if limit is not None:
            take = min(take, limit - deleted)
        # In id order from here on, which is the order the delivery rows are
        # read in below. Selected in ``recorded_at`` order, the key of both
        # indexes the arms read through: ordered by id, the planner may walk
        # the primary key filtering every row, which reads the whole table
        # whenever nothing is due.
        ids = sorted(_candidates(due, take))
        if not ids:
            return deleted
        # At most ``size`` delivery rows, by event: enough to say how many each
        # leading event has, without counting a fan-out's twenty thousand.
        # Ordered by the event alone, which the unique index leads with.
        owners = list(
            DeliveryRecord.objects.filter(event__in=ids)
            .order_by("event_id")
            .values_list("event_id", flat=True)[:size]
        )
        # Every row read belongs to the first event, so it has at least
        # ``size`` and cannot share a batch with itself. Both conditions are
        # held by test_a_batch_is_bounded_by_rows_not_events: without either,
        # an event is sent to be chunked with fewer than ``size`` rows left.
        if len(owners) == size and owners[-1] == ids[0]:
            # A chunk that deleted nothing found the event no longer due, and
            # ends this pass rather than selecting it again: the next prune
            # starts over, and a loop that cannot make progress cannot spin
            # (test_a_stale_selection_is_rechecked_before_a_chunk_too).
            if not _delete_chunk(due, ids[0], size):
                return deleted
            continue
        deleted += _delete_batch(due, _fitting(ids, owners, size))
    return deleted


def _candidates(due: models.QuerySet[Any], take: int) -> list[int]:
    """Ids of up to ``take`` due events, oldest first.

    Its own function so a test can hand the delete a stale selection, which is
    the only way to prove the delete re-checks it.
    """
    return list(due.order_by("recorded_at").values_list("pk", flat=True)[:take])


def _fitting(ids: list[int], owners: list[int], size: int) -> list[int]:
    """The leading events whose rows, their own included, fit in ``size``.

    ``owners`` is the event id of each delivery row read, in event order, and
    stops at ``size`` rows, so once it is full the count is exact only for
    events before the last one it names. That needs no check of its own: the
    rows read before that event, plus the ones of its own that were read, are
    already ``size``, so with its own row it overflows the budget and the walk
    stops there, before any event whose count was never read. The first event
    always fits: the caller has already sent one with ``size`` rows or more to
    be chunked.
    """
    counts = Counter(owners)
    batch: list[int] = []
    used = 0
    for pk in ids:
        used += 1 + counts[pk]
        if used > size:
            break
        batch.append(pk)
    return batch


def _delete_batch(due: models.QuerySet[Any], batch: list[int]) -> int:
    """Delete these events if they are still due, and return how many went."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    # No early exit when this removes fewer than were selected: an event re-owed
    # in between is excluded by the next select, so the loop converges on its
    # own and needs no second way out.
    with transaction.atomic(using=write_alias()):
        # Due is re-checked here, not only in the select. A replay landing in
        # between makes rows owed again, and the cascade would take them with
        # no record that anything was lost - after the operator was told they
        # had been reopened (test_a_stale_selection_is_rechecked_at_the_delete).
        still_due = list(due.filter(pk__in=batch).values_list("pk", flat=True))
        _record_last_successes(DeliveryRecord.objects.filter(event__in=still_due))
        return _delete_events(still_due)


def _delete_events(ids: list[int]) -> int:
    """Delete these events and their delivery rows, and count the events.

    The per-model count, not delete()'s total: that includes the cascaded
    delivery rows, so a caller asking how many events went would be told how
    many rows went.
    """
    from django_domain_events.models.event_record import EventRecord

    return EventRecord.objects.filter(pk__in=ids).delete()[1].get(EventRecord._meta.label, 0)


def _delete_chunk(due: models.QuerySet[Any], event_id: int, size: int) -> int:
    """Delete ``size`` of one due event's delivery rows, leaving the event, and
    return how many went.

    A range on the primary key, up to the ``size``-th row's, rather than a
    list of ids, so the statement binds a handful of parameters however large
    ``size`` is. Finding that row reads the event's delivery rows once, which
    for the largest fan-out is tens of thousands of index entries per chunk.
    The event's due-ness is part of the same statement, so a chunk is deleted
    only while the event is still due
    (test_a_stale_selection_is_rechecked_before_a_chunk_too).
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    with transaction.atomic(using=write_alias()):
        boundary = list(
            DeliveryRecord.objects.filter(event=event_id)
            .order_by("pk")
            .values_list("pk", flat=True)[size - 1 : size]
        )
        chunk = DeliveryRecord.objects.filter(
            event__in=due.filter(pk=event_id).values("pk"),
            pk__lte=max(boundary, default=0),
        )
        _record_last_successes(chunk)
        return chunk.delete()[0]


def _record_last_successes(rows: models.QuerySet[Any]) -> None:
    """Keep each receiver's newest success among ``rows``, which are about to go.

    Monotonic: a batch of older events never moves a receiver's recorded
    success backwards (test_a_prune_never_moves_a_last_success_backwards). An
    insert that ignores an existing row, then an update that applies only
    where it is later - two statements that hold under a concurrent prune,
    where reading the current value and writing the larger would not.
    """
    from django_domain_events.models.receiver_last_success import ReceiverLastSuccess

    newest = dict(
        rows.filter(succeeded_at__isnull=False)
        .order_by()
        .values("receiver_key")
        .annotate(last=models.Max("succeeded_at"))
        .values_list("receiver_key", "last")
    )
    # In key order in both loops: two prunes whose batches differ take these
    # rows in whatever order each GROUP BY returned them, and two transactions
    # taking the same rows in opposite orders deadlock (held by
    # test_two_relays_with_different_batches_never_deadlock).
    ordered = sorted(newest.items())
    ReceiverLastSuccess.objects.bulk_create(
        [ReceiverLastSuccess(receiver_key=key, last_succeeded_at=at) for key, at in ordered],
        ignore_conflicts=True,
    )
    for key, at in ordered:
        ReceiverLastSuccess.objects.filter(receiver_key=key, last_succeeded_at__lt=at).update(
            last_succeeded_at=at
        )
