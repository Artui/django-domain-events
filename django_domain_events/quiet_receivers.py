from __future__ import annotations

from datetime import datetime, timedelta, timezone

from django.db import models

from django_domain_events.registry import registry
from django_domain_events.settings import setting
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.quiet_receiver import QuietReceiver


def quiet_receivers(
    *, within: timedelta | None = None, now: datetime | None = None
) -> list[QuietReceiver]:
    """Declared receivers that have succeeded at nothing inside the window.

    The query an event log makes possible and a signal never will: "this
    receiver has not run since June" is a fact here, not a guess, because every
    durable delivery left a row.

    Driven by the registry rather than by the table, so a receiver that has
    never received anything appears - which is the answer worth having, and
    exactly the one a query over delivery rows alone cannot produce.

    Only DURABLE receivers write rows, so only they are reported. An INLINE or
    ON_COMMIT receiver has no delivery history to be quiet about, and listing it
    as silent forever would train the reader to ignore the output.

    The window defaults to RETENTION_DAYS, which is not a coincidence of
    numbers: past that point the prune has deleted the evidence, so "quiet for
    longer than retention" is the longest answer this can honestly give.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    window = within if within is not None else timedelta(days=setting("RETENTION_DAYS"))
    cutoff = (now or datetime.now(timezone.utc)) - window

    durable = sorted(
        (r for r in registry.receivers() if r.mode is DeliveryMode.DURABLE),
        key=lambda r: r.key,
    )
    rows = (
        DeliveryRecord.objects.filter(
            receiver_key__in=[r.key for r in durable], status=DeliveryStatus.SUCCEEDED
        )
        .values("receiver_key")
        .annotate(last=models.Max("completed_at"))
    )
    last_seen = {row["receiver_key"]: row["last"] for row in rows}

    quiet = []
    for receiver in durable:
        last = last_seen.get(receiver.key)
        if last is not None and last >= cutoff:
            continue
        # The event may be undeclared: registering a receiver for a class with
        # no @event is a check error, not an import error, so this runs in a
        # project that has one.
        event = registry.event_for_class(receiver.event_class)
        quiet.append(
            QuietReceiver(
                key=receiver.key,
                event_name=event.name if event is not None else receiver.event_class.__name__,
                last_succeeded_at=last,
            )
        )
    return quiet
