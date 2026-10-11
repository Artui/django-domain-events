from __future__ import annotations

from datetime import datetime

from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.types.delivery_status import DeliveryStatus


def drain_outbox(
    limit: int | None = None, *, respect_backoff: bool = False, now: datetime | None = None
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

    It never skips a receiver's ``due=``. A row whose due time has not come is
    left alone, because a scheduled reaction is not a wait to be skipped but
    the time the work exists for: a test that makes an offer would otherwise
    expire it in the same breath. ``now`` is how a test moves forward instead -
    ``drain_outbox(now=placed + timedelta(hours=48))`` delivers what is due by
    then, reading the backoff, when respected, against the same moment. The
    receiver still runs on the real clock; ``DeliveryContext.due_at`` is what it
    should compare against.
    """
    return deliver_pending(limit=limit, ignore_backoff=not respect_backoff, now=now)
