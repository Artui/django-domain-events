from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from django.core.checks import Error, Warning
from django.utils.module_loading import import_string

from django_domain_events.registry import registry
from django_domain_events.settings import setting


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
    """No pending delivery names a receiver the registry no longer has.

    Two guards, and both are load-bearing. Without the first this runs under
    ``check``, ``showmigrations`` and ``makemigrations``, which pass no
    databases. Without the second it runs under ``migrate`` - which does pass
    one - and queries a table migrate has not created yet, so the first command
    a new project runs dies and no tables are created at all.
    """
    from django.db import connections

    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.types.delivery_status import DeliveryStatus

    if not databases:
        return []

    table = DeliveryRecord._meta.db_table
    keys: set[str] = set()
    for alias in databases:
        connection = connections[alias]
        with connection.cursor() as cursor:
            if table not in connection.introspection.table_names(cursor):
                continue
        keys |= set(
            DeliveryRecord.objects.using(alias)
            .filter(status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED])
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
