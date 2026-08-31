from __future__ import annotations

from django_domain_events.registry import registry
from django_domain_events.types.registered_receiver import RegisteredReceiver


def what_listens_to(event_class: type) -> list[RegisteredReceiver]:
    """Every receiver declared for one event, sorted by key.

    The question signals cannot answer: a Django signal's receivers are a list
    of weak references keyed by an opaque dispatch uid, so "who reacts to this"
    is answerable only by grepping.
    """
    return sorted(registry.receivers_for(event_class), key=lambda r: r.key)
