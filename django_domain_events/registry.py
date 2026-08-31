"""The single place that knows what exists, and what listens to it.

The registry is the reason this package is not forty lines. A dataclass plus
``Signal.send()`` covers declaring and firing; what it cannot do is answer "what
listens to this", "is anything fired but never declared", or "has this receiver
received anything in ninety days". Those need one object that has seen every
declaration, which is this one.
"""

from __future__ import annotations

from django_domain_events.types.registered_event import RegisteredEvent
from django_domain_events.types.registered_receiver import RegisteredReceiver


class Registry:
    """Declared events and their receivers.

    Module-level mutable state is otherwise forbidden here; this is the one
    exception, and it is the same shape Django's own admin site and checks
    registry use. It is populated at import of each app's ``events`` module and
    read thereafter.
    """

    def __init__(self) -> None:
        self._events_by_class: dict[type, RegisteredEvent] = {}
        self._events_by_name: dict[str, RegisteredEvent] = {}
        self._receivers: dict[str, RegisteredReceiver] = {}

    def register_event(self, entry: RegisteredEvent) -> None:
        """Record a declared event class.

        Both directions are indexed because both are hot: firing looks up by
        class, and the relay looks up by the name on the row.
        """
        existing = self._events_by_name.get(entry.name)
        if existing is not None and existing.event_class is not entry.event_class:
            raise ValueError(
                f"Event name {entry.name!r} is already registered to "
                f"{existing.event_class!r}. "
                f"Two classes sharing a name would make the rows of one decode as "
                f"the other; pass an explicit name= to whichever should change."
            )
        self._events_by_class[entry.event_class] = entry
        self._events_by_name[entry.name] = entry

    def register_receiver(self, receiver: RegisteredReceiver) -> None:
        """Record a declared receiver.

        A duplicate key is rejected rather than overwritten: the key is written
        onto delivery rows, so two receivers sharing one would each be handed
        the other's work with nothing to distinguish them.
        """
        existing = self._receivers.get(receiver.key)
        if existing is not None and existing.func is not receiver.func:
            raise ValueError(
                f"Receiver key {receiver.key!r} is already registered to "
                f"{existing.func!r}. "
                f"Delivery rows address receivers by this key, so it has to name "
                f"exactly one; pass an explicit key=."
            )
        self._receivers[receiver.key] = receiver

    def event_for_class(self, event_class: type) -> RegisteredEvent | None:
        """The entry for a class, or ``None`` if it was never declared."""
        return self._events_by_class.get(event_class)

    def event_for_name(self, name: str) -> RegisteredEvent | None:
        """The entry for a recorded name, or ``None`` if nothing declares it."""
        return self._events_by_name.get(name)

    def receivers_for(self, event_class: type) -> list[RegisteredReceiver]:
        """Every receiver declared for a class, in declaration order.

        Order is insertion order and is not a guarantee the package makes to
        consumers: relying on it across modules would depend on import order,
        which is a property of ``INSTALLED_APPS`` rather than of anything the
        consumer wrote.
        """
        return [r for r in self._receivers.values() if r.event_class is event_class]

    def receiver_for_key(self, key: str) -> RegisteredReceiver | None:
        """The receiver a delivery row addresses, or ``None`` if it is gone."""
        return self._receivers.get(key)

    def events(self) -> list[RegisteredEvent]:
        """Every declared event."""
        return list(self._events_by_class.values())

    def receivers(self) -> list[RegisteredReceiver]:
        """Every declared receiver."""
        return list(self._receivers.values())

    def clear(self) -> None:
        """Forget everything. For tests that declare throwaway events."""
        self._events_by_class.clear()
        self._events_by_name.clear()
        self._receivers.clear()


registry = Registry()
"""The process-wide registry."""
