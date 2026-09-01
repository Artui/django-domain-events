from __future__ import annotations

from datetime import datetime, timezone

from django.db import models

from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.outbox_health import OutboxHealth
from django_domain_events.types.receiver_backlog import ReceiverBacklog
from django_domain_events.utils import TERMINAL


def outbox_health(*, now: datetime | None = None) -> OutboxHealth:
    """How far behind the outbox is, in one query pair.

    The gap the package left until now: ``quiet_receivers()`` answers whether a
    receiver is running, and nothing answered whether the queue is draining.
    Those fail differently - a relay that has been down for an hour has every
    receiver quiet and a backlog climbing, while a single wedged receiver has a
    backlog and everything else fine.

    Owed means "not terminal", the same definition the relay claims by and the
    prune settles by, so this cannot drift from what the relay will actually
    pick up.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    moment = now or datetime.now(timezone.utc)
    owed = DeliveryRecord.objects.exclude(status__in=TERMINAL)

    totals = owed.aggregate(
        owed=models.Count("pk"),
        claimed=models.Count("pk", filter=models.Q(status=DeliveryStatus.CLAIMED)),
        oldest=models.Min("available_at"),
        lapsed=models.Count(
            "pk",
            filter=models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=moment),
        ),
    )
    dead_total = DeliveryRecord.objects.filter(status=DeliveryStatus.DEAD).count()

    # One grouped query for the per-receiver split rather than one per receiver:
    # this is meant to be scraped on a schedule, so its cost is paid forever.
    per_receiver = (
        DeliveryRecord.objects.filter(
            models.Q(status=DeliveryStatus.DEAD) | ~models.Q(status__in=TERMINAL)
        )
        .values("receiver_key")
        .annotate(
            owed=models.Count("pk", filter=~models.Q(status__in=TERMINAL)),
            dead=models.Count("pk", filter=models.Q(status=DeliveryStatus.DEAD)),
            oldest=models.Min("available_at", filter=~models.Q(status__in=TERMINAL)),
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
