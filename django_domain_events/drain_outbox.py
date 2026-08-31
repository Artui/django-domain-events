"""The test helper that runs the real delivery path."""

from __future__ import annotations

from django_domain_events.deliver import deliver_pending
from django_domain_events.types.delivery_status import DeliveryStatus


def drain_outbox(limit: int | None = None) -> dict[DeliveryStatus, int]:
    """Deliver everything owed, from a test, through the production code path.

    Deliberately not a ``task_always_eager`` equivalent. Celery's is a famous
    source of suites that pass while production breaks, because bypassing the
    transport hides both the serialisation boundary and the timing: a payload
    that cannot round-trip, or a receiver that reads state the firing process
    happened to still have, both pass under an eager flag and fail in a worker.

    This runs the same encode, the same decode, and the same acknowledgement as
    the relay. It skips only the waiting.
    """
    return deliver_pending(limit=limit)
