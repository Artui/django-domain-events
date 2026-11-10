from __future__ import annotations

from collections.abc import Collection
from datetime import datetime, timedelta

from django.db import connections, models, transaction

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import DEFAULT_LANE
from django_domain_events.utils import in_project_time, not_yet_due


def claim_batch(
    *,
    worker_id: str,
    now: datetime,
    lease: timedelta,
    limit: int,
    only_ids: list[int] | None = None,
    ignore_backoff: bool = False,
    lane: str | None = None,
    exclude_lanes: Collection[str] = (),
    exclude_ids: Collection[int] = (),
) -> list[int]:
    """Take ownership of up to ``limit`` deliveries and return their ids."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    now = in_project_time(now)
    retryable = models.Q(status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED])
    if not ignore_backoff:
        retryable &= models.Q(available_at__lte=now)
    else:
        retryable &= ~not_yet_due(now)
    owed = retryable | models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=now)
    owed &= _in_lane(lane)
    for paused in sorted(exclude_lanes):
        owed &= ~_in_lane(paused)
    owed &= ~models.Q(pk__in=sorted(exclude_ids))

    alias = write_alias()
    connection = connections[alias]
    with transaction.atomic(using=alias):
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


def _in_lane(lane: str | None) -> models.Q:
    """The rows a relay serving ``lane`` may claim, as one condition."""
    if lane is None:
        return models.Q()
    if lane == DEFAULT_LANE:
        return ~models.Q(receiver_key__in=registry.receiver_keys_in_named_lanes())
    return models.Q(receiver_key__in=registry.receiver_keys_in_lane(lane))


def _locked(queryset: models.QuerySet, *, skip_locked: bool, for_update: bool) -> models.QuerySet:
    """Lock the candidate rows as strongly as the backend allows."""
    if skip_locked:
        return queryset.select_for_update(skip_locked=True)
    if for_update:
        return queryset.select_for_update()
    return queryset
