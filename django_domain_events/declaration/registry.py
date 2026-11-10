from __future__ import annotations

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_event import RegisteredEvent
from django_domain_events.types.registered_receiver import DEFAULT_LANE, RegisteredReceiver

LEASE_HEADROOM = 1.1
"""How far past a receiver's own lease a claim may stamp, as a factor.

A claim stamps its lane's lease (at most ``LEASE_SECONDS``) times this, and a
delivery whose claimed lease still lies between its receiver's lease and that product starts without
extending it: the extension is a write to an indexed column, so it is never a
HOT update. The headroom is what lets more than the first row of a batch start
inside the band, at the cost of a crashed row holding 10 percent longer. Fixed,
not a setting, because its only meaning is WAL against recovery time."""


# The widths of the columns a registration is later written to: ``receiver_key`` on
# ``DeliveryRecord`` and on ``ReceiverLastSuccess``, ``name`` on ``EventRecord``, each
# ``CharField(max_length=255)``. Carried as numbers rather than read off the fields
# because a decorator reaches the registry while apps are still loading - declaring a
# receiver with an explicit ``key=`` works before ``django.setup()`` - and importing a
# model there raises ``AppRegistryNotReady``. Held to the columns by
# test_the_registry_limits_are_the_widths_of_the_columns_that_store_them.
MAX_KEY_LENGTH = 255
MAX_NAME_LENGTH = 255


class Registry:
    """What exists, and what listens to it, indexed both ways because both are hot."""

    def __init__(self) -> None:
        self._events_by_class: dict[type, RegisteredEvent] = {}
        self._events_by_name: dict[str, RegisteredEvent] = {}
        self._receivers: dict[str, RegisteredReceiver] = {}

    def register_event(self, entry: RegisteredEvent) -> None:
        # Shared by every registration route; SQLite would accept a name Postgres refuses.
        if len(entry.name) > MAX_NAME_LENGTH:
            raise ValueError(
                f"Event name {entry.name[:60]!r}... is {len(entry.name)} characters; "
                f"the limit is {MAX_NAME_LENGTH}, the width of the column event rows "
                f"store a name in. A longer one is rejected by the database only when "
                f"an event fires. Set a shorter name=."
            )
        existing = self._events_by_name.get(entry.name)
        if existing is not None and existing.event_class is not entry.event_class:
            raise ValueError(
                f"Event name {entry.name!r} is already registered to "
                f"{existing.event_class!r}. Rows of one would decode as the "
                f"other; pass an explicit name= to whichever should change."
            )
        self._events_by_class[entry.event_class] = entry
        self._events_by_name[entry.name] = entry

    def register_receiver(self, receiver: RegisteredReceiver) -> None:
        if len(receiver.key) > MAX_KEY_LENGTH:
            raise ValueError(
                f"Receiver key {receiver.key[:60]!r}... is {len(receiver.key)} characters; "
                f"the limit is {MAX_KEY_LENGTH}, the width of the column delivery rows "
                f"store a key in, and it applies in every mode so that a key stays valid "
                f"when its mode changes. A longer one is truncated or rejected by the "
                f"database only when an event fires. Set a shorter key=."
            )
        # Compare the whole record: stacked decorators must not silently replace each other.
        existing = self._receivers.get(receiver.key)
        if existing is not None and existing != receiver:
            raise ValueError(
                f"Receiver key {receiver.key!r} is already registered for "
                f"{existing.event_class.__name__} and cannot be reused for "
                f"{receiver.event_class.__name__}. Delivery rows address "
                f"receivers by this key, so it has to name exactly one "
                f"registration; give each an explicit key=."
            )
        self._receivers[receiver.key] = receiver

    def event_for_class(self, event_class: type) -> RegisteredEvent | None:
        return self._events_by_class.get(event_class)

    def event_for_name(self, name: str) -> RegisteredEvent | None:
        return self._events_by_name.get(name)

    def receivers_for(self, event_class: type) -> list[RegisteredReceiver]:
        """The receivers owed an event of this class, including every wildcard."""
        return [
            r
            for r in self._receivers.values()
            if r.event_class is event_class or r.event_class is AnyEvent
        ]

    def receiver_for_key(self, key: str) -> RegisteredReceiver | None:
        return self._receivers.get(key)

    def events(self) -> list[RegisteredEvent]:
        return list(self._events_by_class.values())

    def receivers(self) -> list[RegisteredReceiver]:
        return list(self._receivers.values())

    def receiver_keys_in_lane(self, lane: str) -> list[str]:
        """The keys of the receivers declared in ``lane``, sorted; asked on every claim."""
        return sorted(r.key for r in self._receivers.values() if r.lane == lane)

    def receiver_keys_in_named_lanes(self) -> list[str]:
        """Keys of every receiver in a named lane; the default lane is their complement."""
        return sorted(r.key for r in self._receivers.values() if r.lane != DEFAULT_LANE)

    def lanes(self) -> list[str]:
        """Every lane a relay can serve, sorted: the default and each one declared."""
        return sorted({DEFAULT_LANE} | {r.lane for r in self._receivers.values()})

    def lane_for_key(self, key: str) -> str:
        """The lane a row of ``key`` is claimed in: its receiver's, or the default if none."""
        receiver = self._receivers.get(key)
        return DEFAULT_LANE if receiver is None else receiver.lane

    def claim_lease_seconds(self, lane: str | None, *, default: int, eager: bool = False) -> float:
        """The lease a claim of ``lane`` stamps, in seconds: its base times the headroom."""
        leases = {
            default if r.lease_seconds is None else r.lease_seconds
            for r in self._receivers.values()
            if r.mode is DeliveryMode.DURABLE
            and (r.eager or not eager)
            and (lane is None or r.lane == lane)
        }
        shared = leases.pop() if len(leases) == 1 else default
        return min(shared, default) * LEASE_HEADROOM

    def require_lane(self, lane: str | None) -> None:
        """Refuse a lane no receiver declares: a typo would claim nothing, and look healthy."""
        if lane is not None and lane not in self.lanes():
            raise ValueError(
                f"No receiver is declared in lane {lane!r}, so a relay serving it "
                f"would claim nothing. Declared lanes: {', '.join(self.lanes())}."
            )

    def clear(self) -> None:
        self._events_by_class.clear()
        self._events_by_name.clear()
        self._receivers.clear()


registry = Registry()
