from __future__ import annotations

from django_domain_events.deliver import deliver_pending
from django_domain_events.types.delivery_status import DeliveryStatus


def drain_outbox(limit: int | None = None) -> dict[DeliveryStatus, int]:
    """Deliver everything owed, from a test, through the production code path.

    Deliberately not a ``task_always_eager`` equivalent: bypassing the transport
    hides both the serialisation boundary and the timing, which is how a suite
    passes while production breaks. This skips only the waiting.
    """
    return deliver_pending(limit=limit)
