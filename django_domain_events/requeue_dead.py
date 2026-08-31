from __future__ import annotations

from datetime import datetime, timezone

from django_domain_events.types.delivery_status import DeliveryStatus
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

    dead = DeliveryRecord.objects.filter(status=DeliveryStatus.DEAD)
    if receiver_key is not None:
        dead = dead.filter(receiver_key=receiver_key)
    ids = list(
        dead.order_by("pk").values_list("pk", flat=True)[:limit]
        if limit
        else dead.values_list("pk", flat=True)
    )
    if not ids:
        return 0
    return (
        DeliveryRecord.objects.using(write_alias())
        .filter(pk__in=ids)
        .update(
            status=DeliveryStatus.PENDING,
            attempts=0,
            available_at=datetime.now(timezone.utc),
            claimed_by="",
            claimed_at=None,
            lease_expires_at=None,
            completed_at=None,
        )
    )
