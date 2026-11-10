from __future__ import annotations

from django_domain_events.declaration.registry import registry
from django_domain_events.types.registered_event import RegisteredEvent


def listens_for(receiver_key: str) -> RegisteredEvent | None:
    """The event one receiver is declared for, or None: a wildcard names no single one."""
    receiver = registry.receiver_for_key(receiver_key)
    if receiver is None:
        return None
    return registry.event_for_class(receiver.event_class)
