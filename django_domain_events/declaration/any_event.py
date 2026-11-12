from __future__ import annotations


class AnyEvent:
    """Declare a receiver for every event: ``@receiver(AnyEvent, ...)``.

    A marker, never fired and never instantiated. ``registry.receivers_for``
    returns the receivers declared for an event's class **plus** every receiver
    declared for this one, so a wildcard is matched when an event is fired - not
    when the receiver is declared. That is the whole difference from walking the
    registry at startup and declaring a receiver per event, which silently
    misses every event declared by an app that loads afterwards.

    Meant for a *transport* - something that forwards events without knowing
    what they are, such as a webhook sender or a bridge to a broker. A wildcard
    on its own writes a delivery row for every event the system fires, which is
    why it is usually declared with ``targets=``: a callable returning nothing
    for an event nobody wants writes no row at all.

    The receiver is handed whatever was fired, so its event parameter is
    honestly typed ``object``.

    It appears in the catalogue in a section of its own rather than under every
    event, and ``what_listens_to(OrderPlaced)`` leaves it out; ask
    ``what_listens_to(AnyEvent)`` for the wildcards.
    """
