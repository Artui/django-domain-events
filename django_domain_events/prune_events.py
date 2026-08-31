from __future__ import annotations

from datetime import datetime, timedelta, timezone

from django.db import models, transaction

from django_domain_events.settings import setting
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.write_alias import write_alias

TERMINAL = (DeliveryStatus.SUCCEEDED, DeliveryStatus.DEAD, DeliveryStatus.ORPHANED)


def prune_events(
    older_than: timedelta | None = None,
    *,
    now: datetime | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
) -> int:
    """Delete settled events older than the window, and return how many went.

    An outbox without a prune story becomes the largest table in the database,
    and it becomes it quietly - which is why this ships rather than waiting for
    someone to notice.

    Only *settled* events: one with a delivery still pending, failed or claimed
    is still owed, and deleting it would drop work nobody recorded as lost. An
    event with no delivery rows at all - suppressed, or fired with no durable
    receivers - is settled by definition.

    Deletes in batches. A single statement over a year of rows takes a lock for
    as long as it runs, on the table the relay is trying to claim from.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    window = older_than if older_than is not None else timedelta(days=setting("RETENTION_DAYS"))
    cutoff = (now or datetime.now(timezone.utc)) - window
    size = batch_size if batch_size is not None else setting("BATCH_SIZE")
    alias = write_alias()

    unsettled = DeliveryRecord.objects.filter(event=models.OuterRef("pk")).exclude(
        status__in=TERMINAL
    )
    settled = EventRecord.objects.filter(recorded_at__lt=cutoff).exclude(models.Exists(unsettled))

    deleted = 0
    while limit is None or deleted < limit:
        take = size if limit is None else min(size, limit - deleted)
        ids = list(settled.order_by("pk").values_list("pk", flat=True)[:take])
        if not ids:
            return deleted
        with transaction.atomic(using=alias):
            # Cascade takes the delivery rows, so retention stays a single
            # delete rather than an ordering problem.
            EventRecord.objects.filter(pk__in=ids).delete()
        deleted += len(ids)
    return deleted
