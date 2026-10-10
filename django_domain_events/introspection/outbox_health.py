from __future__ import annotations

from datetime import datetime, timezone

from django.db import models

from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.outbox_health import OutboxHealth
from django_domain_events.types.receiver_backlog import ReceiverBacklog
from django_domain_events.utils import owed


def outbox_health(*, now: datetime | None = None) -> OutboxHealth:
    """How far behind the outbox is.

    The gap the package left until now: ``quiet_receivers()`` answers whether a
    receiver is running, and nothing answered whether the queue is draining.
    Those fail differently - a relay that has been down for an hour has every
    receiver quiet and a backlog climbing, while a single wedged receiver has a
    backlog and everything else fine.

    Owed means "not terminal", which is what the prune settles by and a
    superset of what the relay can claim right now - a row inside its backoff
    window is owed and not yet claimable. The superset is the useful side: this
    cannot report an empty queue while work is still outstanding.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    moment = now or datetime.now(timezone.utc)
    # Every predicate below is a partial index's own condition, word for word,
    # because that is the only form Postgres matches a partial index to: "not
    # terminal" means the same rows and implies none of the conditions, so it
    # read the whole delivered history on every scrape. See ``owed`` for what
    # holds the list of owed statuses complete.
    is_owed = owed()
    is_dead = models.Q(status=DeliveryStatus.DEAD)
    owed_rows = DeliveryRecord.objects.filter(is_owed)

    totals = owed_rows.aggregate(
        owed=models.Count("pk"),
        claimed=models.Count("pk", filter=models.Q(status=DeliveryStatus.CLAIMED)),
        oldest=models.Min("event__recorded_at"),
        lapsed=models.Count(
            "pk",
            filter=models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=moment),
        ),
    )
    dead_total = DeliveryRecord.objects.filter(is_dead).count()

    # One grouped query for the per-receiver split rather than one per receiver:
    # this is meant to be scraped on a schedule, so its cost is paid forever.
    # Three in total: this, the aggregate above, and the dead count.
    per_receiver = (
        DeliveryRecord.objects.filter(is_dead | is_owed)
        .values("receiver_key")
        .annotate(
            owed=models.Count("pk", filter=is_owed),
            dead=models.Count("pk", filter=is_dead),
            oldest=models.Min("event__recorded_at", filter=is_owed),
        )
    )
    backlogs = [
        ReceiverBacklog(
            key=row["receiver_key"],
            owed=row["owed"],
            dead=row["dead"],
            oldest_owed_at=row["oldest"],
        )
        for row in per_receiver
    ]
    backlogs.sort(key=lambda b: (-b.owed, -b.dead, b.key))

    return OutboxHealth(
        owed=totals["owed"],
        claimed=totals["claimed"],
        dead=dead_total,
        oldest_owed_at=totals["oldest"],
        lapsed_leases=totals["lapsed"],
        receivers=tuple(backlogs),
    )
