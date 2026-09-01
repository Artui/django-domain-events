from __future__ import annotations

from django_domain_events.registry import registry
from django_domain_events.types.registered_event import RegisteredEvent


def listens_for(receiver_key: str) -> RegisteredEvent | None:
    """The event one receiver is declared for, or None if no such receiver.

    The inverse direction, and the one an operator needs: a dead-letter row
    names a receiver key, and the next question is always what it was supposed
    to be receiving.

    None covers two cases that look the same from a delivery row - a key never
    declared, and one whose event class was deleted out from under it.
    """
    receiver = registry.receiver_for_key(receiver_key)
    if receiver is None:
        return None
    return registry.event_for_class(receiver.event_class)
