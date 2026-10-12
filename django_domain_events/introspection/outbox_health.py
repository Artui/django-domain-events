from __future__ import annotations

from datetime import datetime, timezone

from django.db import models
from django.db.models.functions import Coalesce

from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.outbox_health import OutboxHealth
from django_domain_events.types.receiver_backlog import ReceiverBacklog
from django_domain_events.utils import not_yet_due, owed


def outbox_health(*, now: datetime | None = None) -> OutboxHealth:
    """How far behind the outbox is.

    The gap the package left until now: ``quiet_receivers()`` answers whether a
    receiver is running, and nothing answered whether the queue is draining.
    Those fail differently - a relay that has been down for an hour has every
    receiver quiet and a backlog climbing, while a single wedged receiver has a
    backlog and everything else fine.

    Owed means "not terminal and due", and a superset of what the relay can
    claim right now - a row inside its backoff window is owed and not yet
    claimable. The superset is the useful side: this cannot report an empty
    queue while work is still outstanding. A row whose due time is still ahead
    of ``now`` is not outstanding yet, so it is counted as ``scheduled``
    instead, and is never what the backlog age is read from: a receiver
    declared with ``due=`` fourteen days out would otherwise hold a five-minute
    alert open for the fortnight.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    moment = now or datetime.now(timezone.utc)
    # Every predicate below is a partial index's own condition, word for word,
    # because that is the only form Postgres matches a partial index to: "not
    # terminal" means the same rows but implies none of the owed or dead
    # conditions, so it read the whole delivered history on every scrape. See
    # ``owed`` for what holds the list of owed statuses complete.
    #
    # The split between due and scheduled is a filter on the aggregates, never
    # on the WHERE clause, so the queries and the indexes that serve them are
    # the ones they were before scheduling existed
    # (test_no_query_outbox_health_sends_scans_the_delivery_table). Counting
    # the scheduled rows costs reading them, through the owed index that holds
    # them anyway.
    is_owed = owed()
    is_dead = models.Q(status=DeliveryStatus.DEAD)
    is_claimed = models.Q(status=DeliveryStatus.CLAIMED)
    waiting = not_yet_due(moment)
    # The negation keeps NULL due_at, which is every row nobody scheduled.
    due = ~waiting
    # When a row became owed: its due time where it has one, and its event's
    # recording otherwise, as give_up_after reads it.
    owed_since = Coalesce("due_at", "event__recorded_at")
    owed_rows = DeliveryRecord.objects.filter(is_owed)

    totals = owed_rows.aggregate(
        owed=models.Count("pk", filter=due),
        scheduled=models.Count("pk", filter=waiting),
        claimed=models.Count("pk", filter=is_claimed),
        oldest=models.Min(owed_since, filter=due),
        lapsed=models.Count(
            "pk",
            filter=models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=moment),
        ),
    )
    dead_total = DeliveryRecord.objects.filter(is_dead).count()

    # One grouped query for the per-receiver split rather than one per receiver:
    # this is meant to be scraped on a schedule, so its cost is paid forever.
    # Three in total: this, the aggregate above, and the dead count. The claimed
    # split is one more aggregate in this query, never a query of its own.
    per_receiver = (
        DeliveryRecord.objects.filter(is_dead | is_owed)
        .values("receiver_key")
        .annotate(
            owed=models.Count("pk", filter=is_owed & due),
            dead=models.Count("pk", filter=is_dead),
            scheduled=models.Count("pk", filter=is_owed & waiting),
            claimed=models.Count("pk", filter=is_claimed),
            oldest=models.Min(owed_since, filter=is_owed & due),
        )
    )
    backlogs = [
        ReceiverBacklog(
            key=row["receiver_key"],
            owed=row["owed"],
            dead=row["dead"],
            oldest_owed_at=row["oldest"],
            scheduled=row["scheduled"],
            claimed=row["claimed"],
        )
        for row in per_receiver
    ]
    # Scheduled work is not a backlog, so it does not rank: a receiver with
    # only scheduled rows sorts after every receiver that is behind.
    backlogs.sort(key=lambda b: (-b.owed, -b.dead, b.key))

    return OutboxHealth(
        owed=totals["owed"],
        claimed=totals["claimed"],
        dead=dead_total,
        oldest_owed_at=totals["oldest"],
        lapsed_leases=totals["lapsed"],
        receivers=tuple(backlogs),
        scheduled=totals["scheduled"],
    )
