from __future__ import annotations

from collections.abc import Iterable

from django.db import models, transaction
from django.utils import timezone

from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.utils import RETRYABLE


def cancel_deliveries(
    event_ids: Iterable[int],
    *,
    receiver_key: str | None = None,
    delivery_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """Withdraw the owed deliveries of these events, so they never run."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    alias = write_alias()
    chunk = setting("BATCH_SIZE")
    counts = {"cancelled": 0, "in_flight": 0}
    for locks, scope in _batches(sorted(set(event_ids)), delivery_ids, chunk):
        if receiver_key is not None:
            scope &= models.Q(receiver_key=receiver_key)
        with transaction.atomic(using=alias):
            list(
                EventRecord.objects.using(alias)
                .select_for_update()
                .filter(locks)
                .order_by("pk")
                .values_list("pk", flat=True)
            )
            now = timezone.now()
            counts["cancelled"] += (
                DeliveryRecord.objects.using(alias)
                .filter(scope, status__in=RETRYABLE)
                .update(status=DeliveryStatus.CANCELLED, completed_at=now)
            )
            counts["in_flight"] += (
                DeliveryRecord.objects.using(alias)
                .filter(scope, status=DeliveryStatus.CLAIMED)
                .count()
            )
    return counts


def _batches(
    events: list[int], delivery_ids: Iterable[int] | None, chunk: int
) -> list[tuple[models.Q, models.Q]]:
    """Each batch as the events to lock and the delivery rows it may write."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    if delivery_ids is None:
        return [
            (models.Q(pk__in=part), models.Q(event__in=part))
            for part in (events[start : start + chunk] for start in range(0, len(events), chunk))
        ]
    named = set(events)
    wanted = sorted(set(delivery_ids))
    rows: list[int] = []
    for start in range(0, len(wanted), chunk):
        rows.extend(
            pk
            for pk, event in DeliveryRecord.objects.filter(
                pk__in=wanted[start : start + chunk]
            ).values_list("pk", "event_id")
            if event in named
        )
    return [
        (
            models.Q(pk__in=DeliveryRecord.objects.filter(pk__in=part).values("event_id")),
            models.Q(pk__in=part),
        )
        for part in (rows[start : start + chunk] for start in range(0, len(rows), chunk))
    ]
