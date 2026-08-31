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
    ignore_backoff: bool = False,
) -> list[int]:
    """Take ownership of up to ``limit`` deliveries and return their ids.

    Claims are leased, not marked: a worker that dies without acknowledging
    becomes re-claimable when its lease lapses, which is the same path as an
    ordinary retry rather than a special case.

    Rows are selected by ``available_at`` and never by primary key. A
    transaction holding a lower id can commit after one holding a higher id, so
    a row can become visible "in the past"; a high-water mark would skip it
    forever.

    ``ignore_backoff`` claims rows whose retry is still scheduled. It exists for
    the test helper, which cannot wait out a jittered hour to observe a retry.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    retryable = models.Q(status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED])
    if not ignore_backoff:
        retryable &= models.Q(available_at__lte=now)
    owed = retryable | models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=now)

    with transaction.atomic():
        candidates = DeliveryRecord.objects.filter(owed).order_by("available_at", "pk")
        if only_ids is not None:
            candidates = candidates.filter(pk__in=only_ids)
        candidates = _locked(
            candidates,
            skip_locked=connection.features.has_select_for_update_skip_locked,
            for_update=connection.features.has_select_for_update,
        )
        ids = list(candidates.values_list("pk", flat=True)[:limit])
        if ids:
            DeliveryRecord.objects.filter(pk__in=ids).update(
                status=DeliveryStatus.CLAIMED,
                claimed_by=worker_id,
                claimed_at=now,
                lease_expires_at=now + lease,
            )
    return ids


def _locked(queryset: models.QuerySet, *, skip_locked: bool, for_update: bool) -> models.QuerySet:
    """Lock the candidate rows as strongly as the backend allows.

    Skipping is preferred so workers do not queue behind each other, but a
    blocking ``FOR UPDATE`` is still correct - it serialises the claim rather
    than losing it. Only a backend with no row locking at all (SQLite) falls
    through unprotected, and that is what ``run_relay`` refuses to start on.

    The capabilities are arguments rather than reads of ``connection.features``
    so every branch is reachable from any backend; otherwise each one could only
    be covered by the database that has it.
    """
    if skip_locked:
        return queryset.select_for_update(skip_locked=True)
    if for_update:
        return queryset.select_for_update()
    return queryset
