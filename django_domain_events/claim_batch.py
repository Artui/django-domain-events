from __future__ import annotations

from datetime import datetime, timedelta

from django.db import connection, models, transaction

from django_domain_events.types.delivery_status import DeliveryStatus


def claim_batch(
    *,
    worker_id: str,
    now: datetime,
    lease: timedelta,
    limit: int,
    only_ids: list[int] | None = None,
    skip_locked: bool | None = None,
) -> list[int]:
    """Take ownership of up to ``limit`` deliveries and return their ids.

    Claims are leased, not marked: a worker that dies without acknowledging
    becomes re-claimable when its lease lapses, which is the same path as an
    ordinary retry rather than a special case.

    Rows are selected by ``available_at`` and never by primary key. A
    transaction holding a lower id can commit after one holding a higher id, so
    a row can become visible "in the past"; a high-water mark would skip it
    forever.

    ``skip_locked`` defaults to whether the backend supports it. Forcing it on a
    backend that does not raises ``NotSupportedError`` from Django rather than
    silently claiming rows another worker holds.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    if skip_locked is None:
        skip_locked = connection.features.has_select_for_update_skip_locked

    owed = models.Q(
        status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED], available_at__lte=now
    ) | models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=now)

    with transaction.atomic():
        candidates = DeliveryRecord.objects.filter(owed).order_by("available_at", "pk")
        if only_ids is not None:
            candidates = candidates.filter(pk__in=only_ids)
        if skip_locked:
            candidates = candidates.select_for_update(skip_locked=True)
        ids = list(candidates.values_list("pk", flat=True)[:limit])
        if ids:
            DeliveryRecord.objects.filter(pk__in=ids).update(
                status=DeliveryStatus.CLAIMED,
                claimed_by=worker_id,
                claimed_at=now,
                lease_expires_at=now + lease,
            )
    return ids
