from __future__ import annotations

from django_domain_events.declaration.registry import registry
from django_domain_events.types.registered_receiver import RegisteredReceiver


def what_listens_to(event_class: type) -> list[RegisteredReceiver]:
    """Every receiver declared for one event, sorted by key, plus every wildcard."""
    return sorted(
        (r for r in registry.receivers() if r.event_class is event_class),
        key=lambda r: r.key,
    )
