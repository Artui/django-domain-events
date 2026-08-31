from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone

from django.db import transaction

from django_domain_events.registry import registry
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.wake import notify_relay
from django_domain_events.write_alias import write_alias


def replay_events(
    event_ids: Iterable[int], *, receiver_keys: Iterable[str] | None = None
) -> dict[str, int]:
    """Make these events owed again, and report what changed.

    The receiver set freezes at fire time, so deploying a new receiver does not
    hand it a backlog of week-old events. That is deliberate - and this is the
    other half of it: replay is an operation somebody invokes, with a name, and
    not an accident of a deploy.

    Two things happen, and they are counted separately because they are
    different decisions. A terminal delivery is *reopened*: it ran, and you want
    it to run again. A receiver with no row for the event is *added*: it did not
    exist when the event fired, and you are choosing to give it the backlog.

    A delivery still in flight is left alone. Reopening a claimed row would hand
    the same work to two receivers, which is the one thing the lease exists to
    prevent.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    wanted = set(receiver_keys) if receiver_keys is not None else None
    alias = write_alias()
    now = datetime.now(timezone.utc)
    counts = {"reopened": 0, "added": 0}

    for record in EventRecord.objects.filter(pk__in=list(event_ids)).order_by("pk"):
        # One transaction per event, not one for the whole call: a collision on
        # any single event would otherwise discard the reopens for every other
        # event the operator asked for.
        with transaction.atomic(using=alias):
            entry = registry.event_for_name(record.name)
            if entry is None:
                continue
            durable = {
                r.key: r
                for r in registry.receivers_for(entry.event_class)
                if r.mode is DeliveryMode.DURABLE and (wanted is None or r.key in wanted)
            }
            keys = set(durable)
            existing = dict(
                DeliveryRecord.objects.filter(event=record, receiver_key__in=keys).values_list(
                    "receiver_key", "status"
                )
            )
            reopen = [k for k, status in existing.items() if status in _TERMINAL]
            # The status predicate is what keeps this from wiping a live lease.
            # Between reading the statuses above and this update, a relay can
            # claim a row - and clearing claimed_by on it would hand the same
            # work to two workers, which is the one thing the lease prevents.
            counts["reopened"] += DeliveryRecord.objects.filter(
                event=record, receiver_key__in=reopen, status__in=_TERMINAL
            ).update(
                status=DeliveryStatus.PENDING,
                attempts=0,
                available_at=now,
                claimed_by="",
                claimed_at=None,
                lease_expires_at=None,
                completed_at=None,
                last_error="",
            )
            missing = keys - set(existing)
            if missing:
                # ignore_conflicts, because a concurrent replay of the same
                # event races the unique constraint on (event, receiver_key) -
                # and losing that race means the row exists, which is what was
                # wanted.
                DeliveryRecord.objects.bulk_create(
                    [
                        DeliveryRecord(
                            event=record,
                            receiver_key=key,
                            max_attempts=durable[key].max_attempts,
                            available_at=now,
                        )
                        for key in sorted(missing)
                    ],
                    ignore_conflicts=True,
                )
                # Counted by asking what is there now rather than by what
                # bulk_create returned: with ignore_conflicts most backends
                # return no primary keys, and a row a concurrent replay created
                # is owed either way, which is what the operator asked for.
                counts["added"] += DeliveryRecord.objects.filter(
                    event=record, receiver_key__in=missing
                ).count()
    if counts["reopened"] or counts["added"]:
        # The operations make rows owed just as fire() does, so they wake a
        # waiting relay too; otherwise replayed work sits until the next poll.
        notify_relay()
    return counts


_TERMINAL = (DeliveryStatus.SUCCEEDED, DeliveryStatus.DEAD, DeliveryStatus.ORPHANED)
