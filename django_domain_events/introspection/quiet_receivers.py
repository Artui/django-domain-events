from __future__ import annotations

from datetime import datetime, timedelta, timezone

from django.db import models

from django_domain_events.declaration.registry import registry
from django_domain_events.settings import setting
from django_domain_events.types.delivery_mode import DeliveryMode
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

    Read off ``succeeded_at`` rather than ``completed_at``, and with no status
    filter at all. Both of those describe the *current* cycle: replay and
    requeue reopen a row and clear them, so an operator who replays yesterday's
    events would then be told the receiver had never run. ``Max`` ignores nulls,
    so a receiver whose every delivery failed still reads as never having
    succeeded without a predicate saying so.

    The rows are not the whole record: the prune deletes them, within a sweep
    for an event declared to delete on consumption, and writes each receiver's
    newest success among what it deleted to ``ReceiverLastSuccess`` as it goes.
    The answer is the later of that and the live rows, so a receiver whose
    every event has been pruned still reports when it last ran.

    The window defaults to RETENTION_DAYS, the window an ordinary event's
    deliveries are kept for.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.receiver_last_success import ReceiverLastSuccess

    window = within if within is not None else timedelta(days=setting("RETENTION_DAYS"))
    cutoff = (now or datetime.now(timezone.utc)) - window

    durable = sorted(
        (r for r in registry.receivers() if r.mode is DeliveryMode.DURABLE),
        key=lambda r: r.key,
    )
    # One query for every receiver: the table holds a row per receiver key the
    # prune has ever deleted a delivery of, so it is the size of the registry.
    pruned = dict(
        ReceiverLastSuccess.objects.filter(receiver_key__in=[r.key for r in durable]).values_list(
            "receiver_key", "last_succeeded_at"
        )
    )
    quiet = []
    for receiver in durable:
        # One query per receiver rather than one grouped query, on purpose.
        # ``MAX`` over a single key is a single descent of ``dde_last_success``
        # from the end of that key's range - Postgres rewrites it into a
        # backward index scan under ``LIMIT 1`` - so each costs the same
        # however many rows the receiver has ever had. Grouped by key, the
        # same answer reads every one of those rows, which on a fan-out
        # receiver is the whole table.
        live = DeliveryRecord.objects.filter(receiver_key=receiver.key).aggregate(
            last=models.Max("succeeded_at")
        )["last"]
        last = max((at for at in (live, pruned.get(receiver.key)) if at is not None), default=None)
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
