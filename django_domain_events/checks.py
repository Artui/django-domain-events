from __future__ import annotations

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


def check_no_orphaned_deliveries(**kwargs: Any) -> list[Any]:
    """No pending delivery names a receiver the registry no longer has."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.types.delivery_status import DeliveryStatus

    keys = set(
        DeliveryRecord.objects.filter(status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED])
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
