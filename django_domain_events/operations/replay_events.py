from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from django.db import transaction
from django.utils import timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.wake import notify_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import (
    TERMINAL,
    decode_payload,
    resolve_due,
    resolve_targets,
    target_digest,
)


def replay_events(
    event_ids: Iterable[int], *, receiver_keys: Iterable[str] | None = None
) -> dict[str, int]:
    """Make these events owed again, and report what changed."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    wanted = set(receiver_keys) if receiver_keys is not None else None
    alias = write_alias()
    now = timezone.now()
    counts = {"reopened": 0, "added": 0}

    for record in EventRecord.objects.filter(pk__in=list(event_ids)).order_by("pk"):
        with transaction.atomic(using=alias):
            if (
                not EventRecord.objects.using(alias)
                .select_for_update()
                .filter(pk=record.pk)
                .exists()
            ):
                continue
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
                targets = {
                    target_digest(target): target
                    for target in _targets_now(receiver, record, entry.event_class)
                }
                existing = {
                    bytes(digest): status
                    for digest, status in DeliveryRecord.objects.filter(
                        event=record, receiver_key=receiver.key, target_digest__in=targets
                    ).values_list("target_digest", "status")
                }
                reopen = [d for d, status in existing.items() if status in TERMINAL]
                when = _due_now(receiver, record, entry.event_class, now)
                counts["reopened"] += DeliveryRecord.objects.filter(
                    event=record,
                    receiver_key=receiver.key,
                    target_digest__in=reopen,
                    status__in=TERMINAL,
                ).update(
                    status=DeliveryStatus.PENDING,
                    attempts=0,
                    available_at=when,
                    due_at=when,
                    claimed_by="",
                    claimed_at=None,
                    lease_expires_at=None,
                    completed_at=None,
                    last_error="",
                )
                missing = [digest for digest in targets if digest not in existing]
                if missing:
                    DeliveryRecord.objects.bulk_create(
                        [
                            DeliveryRecord(
                                event=record,
                                receiver_key=receiver.key,
                                target=targets[digest],
                                max_attempts=receiver.max_attempts,
                                available_at=when,
                                due_at=when,
                            )
                            for digest in missing
                        ],
                        ignore_conflicts=True,
                    )
                    counts["added"] += DeliveryRecord.objects.filter(
                        event=record, receiver_key=receiver.key, target_digest__in=missing
                    ).count()
    if counts["reopened"] or counts["added"]:
        notify_relay()
    return counts


def _targets_now(receiver: RegisteredReceiver, record: Any, event_class: type) -> list[str]:
    """The targets one receiver is owed this event at replay time."""
    if receiver.targets is None:
        return [""]
    context = DeliveryContext(
        event_id=record.pk,
        event_name=record.name,
        attempt=1,
        actor_key=record.actor_key,
        actor_label=record.actor_label,
        scope=record.scope,
        receiver_key=receiver.key,
    )
    event = decode_payload(event_class, record.payload, record.version)
    return resolve_targets(receiver.key, receiver.targets, event, context)


def _due_now(
    receiver: RegisteredReceiver, record: Any, event_class: type, now: datetime
) -> datetime:
    """When a row this replay reopens or adds for one receiver becomes due."""
    if receiver.due is None:
        return now
    event = (
        None
        if isinstance(receiver.due, timedelta)
        else decode_payload(event_class, record.payload, record.version)
    )
    return resolve_due(receiver.key, receiver.due, event, record.occurred_at, now)
