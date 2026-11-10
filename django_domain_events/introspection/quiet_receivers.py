from __future__ import annotations

from datetime import datetime, timedelta

from django.db import models
from django.utils import timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.settings import setting
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.quiet_receiver import QuietReceiver
from django_domain_events.utils import in_project_time


def quiet_receivers(
    *, within: timedelta | None = None, now: datetime | None = None
) -> list[QuietReceiver]:
    """Declared receivers that have succeeded at nothing inside the window."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.receiver_last_success import ReceiverLastSuccess

    window = within if within is not None else timedelta(days=setting("RETENTION_DAYS"))
    cutoff = (in_project_time(now) if now is not None else timezone.now()) - window

    durable = sorted(
        (r for r in registry.receivers() if r.mode is DeliveryMode.DURABLE),
        key=lambda r: r.key,
    )
    pruned = dict(
        ReceiverLastSuccess.objects.filter(receiver_key__in=[r.key for r in durable]).values_list(
            "receiver_key", "last_succeeded_at"
        )
    )
    quiet = []
    for receiver in durable:
        live = DeliveryRecord.objects.filter(receiver_key=receiver.key).aggregate(
            last=models.Max("succeeded_at")
        )["last"]
        last = max((at for at in (live, pruned.get(receiver.key)) if at is not None), default=None)
        if last is not None and last >= cutoff:
            continue
        event = registry.event_for_class(receiver.event_class)
        quiet.append(
            QuietReceiver(
                key=receiver.key,
                event_name=event.name if event is not None else receiver.event_class.__name__,
                last_succeeded_at=last,
            )
        )
    return quiet
