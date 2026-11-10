from __future__ import annotations

from datetime import datetime

from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.types.delivery_status import DeliveryStatus


def drain_outbox(
    limit: int | None = None, *, respect_backoff: bool = False, now: datetime | None = None
) -> dict[DeliveryStatus, int]:
    """Deliver everything owed, from a test, through the production code path."""
    return deliver_pending(limit=limit, ignore_backoff=not respect_backoff, now=now)
