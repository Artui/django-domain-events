from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

from django_domain_events.operations.prune_with_counts import prune_with_counts


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
