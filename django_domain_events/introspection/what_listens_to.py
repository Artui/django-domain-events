from __future__ import annotations

from django_domain_events.declaration.registry import registry
from django_domain_events.types.registered_receiver import RegisteredReceiver


def what_listens_to(event_class: type) -> list[RegisteredReceiver]:
    """Every receiver declared for one event, sorted by key - plus every wildcard.

    The question signals cannot answer: a Django signal's receivers are a list
    of weak references keyed by an opaque dispatch uid, so "who reacts to this"
    is answerable only by grepping.

    The wildcards are the "plus", and they are not repeated here. A receiver
    declared for ``AnyEvent`` receives this event as it receives every other,
    and listing a transport under every event answers the question two hundred
    times without saying anything about this one. Ask for them directly with
    ``what_listens_to(AnyEvent)``, which returns exactly those.
    """
    return sorted(
        (r for r in registry.receivers() if r.event_class is event_class),
        key=lambda r: r.key,
    )
