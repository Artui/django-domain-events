from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, NamedTuple

from django.db import models, transaction
from django.utils import timezone

from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.retention import Retention
from django_domain_events.utils import TERMINAL, in_project_time

_EVENTS_PER_BATCH = 500
"""The most events one batch names, whatever the row budget allows.

A batch is a list of ids bound as parameters, and a run of events with no
delivery rows would otherwise put ``PRUNE_BATCH_ROWS`` of them in one
statement - past the parameter limit of an older SQLite at the default."""


class Pruned(NamedTuple):
    """What one prune deleted: events, and the delivery rows that went with them."""

    events: int
    deliveries: int


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
      delivery succeeded or was cancelled) or ``Retention.SETTLED`` (every
      delivery terminal).
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
    return prune_with_counts(
        older_than, now=now, batch_size=batch_size, limit=limit, stop=stop
    ).events


def prune_with_counts(
    older_than: timedelta | None = None,
    *,
    now: datetime | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
    stop: Callable[[], bool] | None = None,
) -> Pruned:
    """``prune_events``, counting the delivery rows it deleted as well."""
    from django_domain_events.models.event_record import EventRecord

    size = batch_size if batch_size is not None else setting("PRUNE_BATCH_ROWS")
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ValueError(f"batch_size must be a positive number of rows, not {size!r}.")
    moment = in_project_time(now) if now is not None else timezone.now()
    window = older_than if older_than is not None else timedelta(days=setting("RETENTION_DAYS"))

    unsettled = _deliveries_of_each().exclude(status__in=TERMINAL)
    unsucceeded = (
        _deliveries_of_each()
        .exclude(status=DeliveryStatus.SUCCEEDED)
        .exclude(status=DeliveryStatus.CANCELLED)
    )
    arms = [
        EventRecord.objects.filter(delete_when=Retention.SUCCEEDED.value).exclude(
            models.Exists(unsucceeded)
        ),
        EventRecord.objects.filter(delete_when=Retention.SETTLED.value).exclude(
            models.Exists(unsettled)
        ),
        EventRecord.objects.filter(retention_seconds__isnull=False)
        .alias(expires_at=models.F("recorded_at") + _Seconds(models.F("retention_seconds")))
        .filter(expires_at__lt=moment)
        .exclude(models.Exists(unsettled)),
        EventRecord.objects.filter(
            retention_seconds__isnull=True, recorded_at__lt=moment - window
        ).exclude(models.Exists(unsettled)),
    ]

    total = Pruned(0, 0)
    for due in arms:
        more = _prune(due, size, None if limit is None else limit - total.events, stop)
        total = Pruned(total.events + more.events, total.deliveries + more.deliveries)
    return total


class _Seconds(models.Func):
    """A column of whole seconds as a duration, in the backend's own form."""

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
) -> Pruned:
    """Delete due events in batches, until none is left, ``limit`` is hit, or ``stop`` says so."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    deleted = 0
    rows = 0
    while (limit is None or deleted < limit) and not (stop is not None and stop()):
        take = min(size, _EVENTS_PER_BATCH)
        if limit is not None:
            take = min(take, limit - deleted)
        ids = sorted(_candidates(due, take))
        if not ids:
            return Pruned(deleted, rows)
        owners = list(
            DeliveryRecord.objects.filter(event__in=ids)
            .order_by("event_id")
            .values_list("event_id", flat=True)[:size]
        )
        if len(owners) == size and owners[-1] == ids[0]:
            chunk = _delete_chunk(due, ids[0], size)
            if not chunk:
                return Pruned(deleted, rows)
            rows += chunk
            continue
        gone = _delete_batch(due, _fitting(ids, owners, size))
        deleted += gone.events
        rows += gone.deliveries
    return Pruned(deleted, rows)


def _candidates(due: models.QuerySet[Any], take: int) -> list[int]:
    """Ids of up to ``take`` due events, oldest first."""
    return list(due.order_by("recorded_at").values_list("pk", flat=True)[:take])


def _fitting(ids: list[int], owners: list[int], size: int) -> list[int]:
    """The leading events whose rows, their own included, fit in ``size``."""
    counts = Counter(owners)
    batch: list[int] = []
    used = 0
    for pk in ids:
        used += 1 + counts[pk]
        if used > size:
            break
        batch.append(pk)
    return batch


def _delete_batch(due: models.QuerySet[Any], batch: list[int]) -> Pruned:
    """Delete these events if they are still due, and count what went."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    with transaction.atomic(using=write_alias()):
        locked = _lock_events(batch)
        still_due = list(due.filter(pk__in=locked).values_list("pk", flat=True))
        _record_last_successes(DeliveryRecord.objects.filter(event__in=still_due))
        return _delete_events(still_due)


def _lock_events(ids: list[int]) -> list[int]:
    """Lock these event rows, in primary key order, and return the ones still there."""
    from django_domain_events.models.event_record import EventRecord

    return list(
        EventRecord.objects.using(write_alias())
        .select_for_update()
        .filter(pk__in=ids)
        .order_by("pk")
        .values_list("pk", flat=True)
    )


def _delete_events(ids: list[int]) -> Pruned:
    """Delete these events and their delivery rows, and count both."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    per_model = EventRecord.objects.filter(pk__in=ids).delete()[1]
    return Pruned(
        per_model.get(EventRecord._meta.label, 0), per_model.get(DeliveryRecord._meta.label, 0)
    )


def _delete_chunk(due: models.QuerySet[Any], event_id: int, size: int) -> int:
    """Delete the delivery rows of one due event, leaving the event, and count them."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    with transaction.atomic(using=write_alias()):
        _lock_events([event_id])
        if not due.filter(pk=event_id).exists():
            return 0
        boundary = list(
            DeliveryRecord.objects.filter(event=event_id)
            .order_by("pk")
            .values_list("pk", flat=True)[size - 1 : size]
        )
        chunk = DeliveryRecord.objects.filter(event=event_id, pk__lte=max(boundary, default=0))
        _record_last_successes(chunk)
        return chunk.delete()[0]


def _record_last_successes(rows: models.QuerySet[Any]) -> None:
    """Keep each receiver's newest success among ``rows``, which are about to go."""
    from django_domain_events.models.receiver_last_success import ReceiverLastSuccess

    newest = dict(
        rows.filter(succeeded_at__isnull=False)
        .order_by()
        .values("receiver_key")
        .annotate(last=models.Max("succeeded_at"))
        .values_list("receiver_key", "last")
    )
    ordered = sorted(newest.items())
    ReceiverLastSuccess.objects.bulk_create(
        [ReceiverLastSuccess(receiver_key=key, last_succeeded_at=at) for key, at in ordered],
        ignore_conflicts=True,
    )
    for key, at in ordered:
        ReceiverLastSuccess.objects.filter(receiver_key=key, last_succeeded_at__lt=at).update(
            last_succeeded_at=at
        )
