from __future__ import annotations

from datetime import datetime, timezone

from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.wake import notify_relay
from django_domain_events.write_alias import write_alias


def requeue_dead(*, receiver_key: str | None = None, limit: int | None = None) -> int:
    """Give dead-lettered deliveries their attempt budget back.

    Dead is where a delivery stops on its own; it is not where it stops for
    good. Attempts reset to zero rather than staying spent, because a row
    requeued at its limit dead-letters again on the first failure and the
    operator learns nothing they did not already know.

    Scoped by receiver, because the usual reason to requeue is that one
    downstream was broken and now is not.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    if limit is not None and limit < 0:
        raise ValueError(f"limit cannot be negative, got {limit}")

    dead = DeliveryRecord.objects.filter(status=DeliveryStatus.DEAD).order_by("pk")
    if receiver_key is not None:
        dead = dead.filter(receiver_key=receiver_key)

    # `is not None`, not truthiness: limit=0 is an operator asking for the
    # smallest possible blast radius, and reading it as "no limit" gives them
    # the largest one.
    ids = list(
        dead.values_list("pk", flat=True)[:limit]
        if limit is not None
        else dead.values_list("pk", flat=True)
    )

    now = datetime.now(timezone.utc)
    chunk = setting("BATCH_SIZE")
    requeued = 0
    for start in range(0, len(ids), chunk):
        # Chunked: SQLite refuses more than 32,766 parameters in one statement,
        # and a dead-letter table past that is an ordinary outcome of one bad
        # deploy. Postgres would take every row lock in a single statement.
        requeued += (
            DeliveryRecord.objects.using(write_alias())
            .filter(pk__in=ids[start : start + chunk], status=DeliveryStatus.DEAD)
            .update(
                status=DeliveryStatus.PENDING,
                attempts=0,
                available_at=now,
                claimed_by="",
                claimed_at=None,
                lease_expires_at=None,
                completed_at=None,
                last_error="",
            )
        )
    if requeued:
        notify_relay()
    return requeued
