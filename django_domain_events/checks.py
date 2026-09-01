from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from django.core.checks import Error, Warning
from django.db import models
from django.utils.module_loading import import_string

from django_domain_events.registry import registry
from django_domain_events.settings import setting
from django_domain_events.utils import TERMINAL, has_table


def check_receivers_have_events(**kwargs: Any) -> list[Any]:
    """Every receiver listens for something that was declared.

    Nothing else would say so: the event cannot be fired, so there is no failure
    to observe, only silence.
    """
    problems = []
    for receiver in registry.receivers():
        if registry.event_for_class(receiver.event_class) is None:
            problems.append(
                Error(
                    f"Receiver {receiver.key!r} listens for "
                    f"{receiver.event_class.__name__}, which is not registered.",
                    hint="Decorate the event class with @event, or delete the receiver.",
                    id="django_domain_events.E001",
                )
            )
    return problems


def check_codec_dependency_is_installed(**kwargs: Any) -> list[Any]:
    """The configured codec can be imported.

    At startup rather than when an event fires: a codec is imported lazily, so
    the first symptom of a missing extra would otherwise be a failed delivery.
    """
    path = setting("CODEC")
    try:
        import_string(path)
    except ImportError as exc:
        return [
            Error(
                f"CODEC is set to {path!r}, which cannot be imported: {exc}",
                hint=(
                    "DaciteCodec needs the 'dacite' extra: "
                    "pip install 'django-domain-events[dacite]'"
                ),
                id="django_domain_events.E002",
            )
        ]
    return []


def check_no_orphaned_deliveries(
    *, databases: Sequence[str] | None = None, **kwargs: Any
) -> list[Any]:
    """No delivery still owed names a receiver the registry no longer has.

    Owed means "not terminal", the same definition the relay claims by and the
    prune settles by. Listing the owed statuses instead is how this check came
    to omit CLAIMED: a worker that died between claiming a row and the deploy
    that deleted its receiver leaves the row claimed with a lapsed lease, and it
    read as settled until a relay happened to reclaim it.

    Two guards, and both are load-bearing. Without the first this runs under
    ``check``, ``showmigrations`` and ``makemigrations``, which pass no
    databases. Without the second it runs under ``migrate`` - which does pass
    one - and queries a table migrate has not created yet, so the first command
    a new project runs dies and no tables are created at all.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    if not databases:
        return []

    table = DeliveryRecord._meta.db_table
    keys: set[str] = set()
    for alias in databases:
        if not has_table(alias, table):
            continue
        keys |= set(
            DeliveryRecord.objects.using(alias)
            .exclude(status__in=TERMINAL)
            .values_list("receiver_key", flat=True)
            .distinct()
        )
    missing = sorted(k for k in keys if registry.receiver_for_key(k) is None)
    if not missing:
        return []
    return [
        Warning(
            f"Delivery rows are waiting for receivers that no longer exist: {', '.join(missing)}.",
            hint=(
                "Restore the receiver under its old key=, or let the rows resolve "
                "to ORPHANED on the next delivery pass."
            ),
            id="django_domain_events.W001",
        )
    ]


def check_recorded_events_are_declared(
    *, databases: Sequence[str] | None = None, **kwargs: Any
) -> list[Any]:
    """No event still owed names something the registry cannot decode.

    Renaming an event is the case this catches and the orphan warning cannot:
    the receivers keep their keys, so nothing looks orphaned, while every row
    written under the old name now decodes to nothing and dead-letters one
    attempt budget at a time.

    Limited to rows still owed. A settled row naming a retired event is
    history, and warning about history every time ``check`` runs teaches the
    reader to skip the output.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    if not databases:
        return []

    table = EventRecord._meta.db_table
    owed = DeliveryRecord.objects.filter(event=models.OuterRef("pk")).exclude(status__in=TERMINAL)
    names: set[str] = set()
    for alias in databases:
        # One table answers for both: they are created by the same
        # migration, so neither exists without the other.
        if not has_table(alias, table):
            continue
        names |= set(
            EventRecord.objects.using(alias)
            .filter(models.Exists(owed))
            .values_list("name", flat=True)
            .distinct()
        )
    missing = sorted(n for n in names if registry.event_for_name(n) is None)
    if not missing:
        return []
    return [
        Warning(
            f"Deliveries are owed for events no longer declared: {', '.join(missing)}.",
            hint=(
                "Most often a renamed event. Pin the old identity with "
                "@event(name=...) on the class that replaced it, or replay the "
                "rows under the new name."
            ),
            id="django_domain_events.W002",
        )
    ]
