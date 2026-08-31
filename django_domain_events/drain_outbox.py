from __future__ import annotations

from django_domain_events.deliver import deliver_pending
from django_domain_events.types.delivery_status import DeliveryStatus


def drain_outbox(
    limit: int | None = None, *, respect_backoff: bool = False
) -> dict[DeliveryStatus, int]:
    """Deliver everything owed, from a test, through the production code path.

    Deliberately not a ``task_always_eager`` equivalent: bypassing the transport
    hides both the serialisation boundary and the timing, which is how a suite
    passes while production breaks. This runs the same claim, encode, decode and
    acknowledgement as the relay, and skips only the waiting.

    Skipping the waiting includes the retry backoff, which is why this ignores
    it by default: a failed delivery is scheduled a jittered interval ahead - up
    to an hour - and a test cannot sit that out. Pass ``respect_backoff=True``
    to assert the schedule itself.
    """
    return deliver_pending(limit=limit, ignore_backoff=not respect_backoff)
