from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from django.db import transaction

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.wake import notify_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import decode_payload, resolve_targets, target_digest


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

    A receiver declared with ``targets=`` has its callable **called again**, so
    a replay goes to the targets that exist now: a target it still returns is
    reopened or added exactly as a receiver is, and one it no longer returns is
    left as it was - neither reopened nor counted - because the callable has
    just said it is not owed this event. That includes a blank row written
    before the receiver gained ``targets=``. A replay is a new delivery, not a
    re-run of an old one, and a target registered after the event was fired
    receiving it is the point rather than a side effect.

    Calling the callable means rebuilding the event, so a fan-out receiver's
    replay fails loudly on a payload that no longer decodes, where a plain
    receiver's reopened row would dead-letter in the relay instead. Either is
    raised to the operator who asked; only the second waits to be found.
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
            durable = sorted(
                (
                    r
                    for r in registry.receivers_for(entry.event_class)
                    if r.mode is DeliveryMode.DURABLE and (wanted is None or r.key in wanted)
                ),
                key=lambda r: r.key,
            )
            for receiver in durable:
                # Keyed by digest, because the digest is what the unique index
                # covers. The text is in no index, so looking rows up by it
                # would scan every delivery of the event.
                targets = {
                    target_digest(target): target
                    for target in _targets_now(receiver, record, entry.event_class)
                }
                existing = dict(
                    DeliveryRecord.objects.filter(
                        event=record, receiver_key=receiver.key, target_digest__in=targets
                    ).values_list("target_digest", "status")
                )
                reopen = [d for d, status in existing.items() if status in _TERMINAL]
                # The status predicate is what keeps this from wiping a live
                # lease. Between reading the statuses above and this update, a
                # relay can claim a row - and clearing claimed_by on it would
                # hand the same work to two workers, which is the one thing the
                # lease prevents.
                counts["reopened"] += DeliveryRecord.objects.filter(
                    event=record,
                    receiver_key=receiver.key,
                    target_digest__in=reopen,
                    status__in=_TERMINAL,
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
                missing = [digest for digest in targets if digest not in existing]
                if missing:
                    # ignore_conflicts, because a concurrent replay of the same
                    # event races the unique constraint on (event, receiver_key,
                    # target_digest) - and losing that race means the row exists,
                    # which is what was wanted.
                    DeliveryRecord.objects.bulk_create(
                        [
                            DeliveryRecord(
                                event=record,
                                receiver_key=receiver.key,
                                target=targets[digest],
                                max_attempts=receiver.max_attempts,
                                available_at=now,
                            )
                            for digest in missing
                        ],
                        ignore_conflicts=True,
                    )
                    # Counted by asking what is there now rather than by what
                    # bulk_create returned: with ignore_conflicts most backends
                    # return no primary keys, and a row a concurrent replay
                    # created is owed either way, which is what the operator
                    # asked for.
                    counts["added"] += DeliveryRecord.objects.filter(
                        event=record, receiver_key=receiver.key, target_digest__in=missing
                    ).count()
    if counts["reopened"] or counts["added"]:
        # The operations make rows owed just as fire() does, so they wake a
        # waiting relay too; otherwise replayed work sits until the next poll.
        notify_relay()
    return counts


def _targets_now(receiver: RegisteredReceiver, record: Any, event_class: type) -> list[str]:
    """The targets one receiver is owed this event at replay time.

    The blank target for a receiver without ``targets=``, which is the row it
    has always had. For a fan-out receiver, whatever its callable returns now,
    handed the same context ``fire()`` built - attempt one, because a replayed
    delivery starts its budget again.
    """
    if receiver.targets is None:
        return [""]
    context = DeliveryContext(
        event_id=record.pk,
        event_name=record.name,
        attempt=1,
        actor_key=record.actor_key,
        actor_label=record.actor_label,
        scope=record.scope,
    )
    event = decode_payload(event_class, record.payload, record.version)
    return resolve_targets(receiver.key, receiver.targets, event, context)


_TERMINAL = (DeliveryStatus.SUCCEEDED, DeliveryStatus.DEAD, DeliveryStatus.ORPHANED)
