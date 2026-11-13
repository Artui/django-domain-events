from __future__ import annotations

from django.db import models

from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.delivery_tally import DeliveryTally
from django_domain_events.utils import TERMINAL


def delivery_tally(event_id: int) -> DeliveryTally:
    """How one event's deliveries stand, counted by outcome in a single query."""
    # Models cannot load while the package root is imported, before apps are ready.
    from django_domain_events.models.delivery_record import DeliveryRecord

    rows = (
        DeliveryRecord.objects.filter(event_id=event_id)
        .values("status")
        .annotate(n=models.Count("pk"))
    )
    counts = {DeliveryStatus(row["status"]): row["n"] for row in rows}
    return DeliveryTally(
        owed=sum(n for status, n in counts.items() if status not in TERMINAL),
        succeeded=counts.get(DeliveryStatus.SUCCEEDED, 0),
        cancelled=counts.get(DeliveryStatus.CANCELLED, 0),
        dead=counts.get(DeliveryStatus.DEAD, 0),
        orphaned=counts.get(DeliveryStatus.ORPHANED, 0),
        total=sum(counts.values()),
    )
