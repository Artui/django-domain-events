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


class Registry:
    """What exists, and what listens to it.

    Both directions are indexed because both are hot: firing looks up by class,
    delivery looks up by the name written on the row.
    """

    def __init__(self) -> None:
        self._events_by_class: dict[type, RegisteredEvent] = {}
        self._events_by_name: dict[str, RegisteredEvent] = {}
        self._receivers: dict[str, RegisteredReceiver] = {}

    def register_event(self, entry: RegisteredEvent) -> None:
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
        # Equality, not identity of the callable. Tolerating "same function"
        # was meant to survive a double import, but stacked decorators are
        # exactly that shape - one function, two events, one derived key - so
        # the second registration silently replaced the first and an event
        # fired to nothing. Comparing the whole record keeps double imports
        # working and catches stacking, a changed mode and a changed
        # max_attempts alike.
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
        """Every receiver an event of this class is owed to, in declaration order.

        The ones declared for the class, **and** every wildcard declared for
        ``AnyEvent``. The wildcard is matched here, at the moment of asking,
        rather than expanded when it is declared - which is what makes it reach
        an event declared by an app that loaded after it.
        """
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
        """The keys of the receivers declared in ``lane``, sorted.

        What a relay serving a named lane claims. Asked on every claim rather
        than cached, so membership is whatever the registry says at that moment.
        """
        return sorted(r.key for r in self._receivers.values() if r.lane == lane)

    def receiver_keys_in_named_lanes(self) -> list[str]:
        """The keys of every receiver declared in a lane other than the default.

        The default lane is phrased as *not these* rather than as the receivers
        declared in it, because that is what reaches a row whose receiver was
        deleted: it names no lane at all, and it still has to drain.
        """
        return sorted(r.key for r in self._receivers.values() if r.lane != DEFAULT_LANE)

    def lanes(self) -> list[str]:
        """Every lane a relay can serve, sorted: the default and each one declared."""
        return sorted({DEFAULT_LANE} | {r.lane for r in self._receivers.values()})

    def claim_lease_seconds(self, lane: str | None, *, default: int, eager: bool = False) -> float:
        """The lease a claim of ``lane`` stamps, in seconds: its base times the headroom.

        The base is the effective lease - ``lease_seconds``, or ``default``
        where none is declared - shared by every durable receiver the claim can
        take, when they all share one, and ``default`` otherwise: a lane mixing
        leases, or no durable receiver at all. A named lane's receivers are
        those declared in it, the default lane's are those declared in no named
        one, and None is every receiver. ``eager`` narrows them to the eager
        ones, for the attempt after commit, which claims only their rows.

        The base never exceeds ``default``. A lane of receivers declaring a
        longer lease than ``LEASE_SECONDS`` is claimed under the setting, as the
        default lane is, and each row extended to its own lease when it starts: stamped
        with theirs, the unstarted tail of a batch a relay was killed holding
        would wait out half an hour rather than five and a half minutes
        (``test_a_long_lease_lane_recovers_in_the_setting``). A lease longer than
        the setting plus the headroom gains no skip, and no lane loses recovery
        time to a long lease.

        ``default`` is ``LEASE_SECONDS``, passed in rather than read here so the
        registry stays free of settings. Asked on every claim, as lane
        membership is, so it is whatever the declarations say at that moment.

        Each filter is one arc with the others, so each names its test in
        ``tests/declaration/test_registry_claim_lease.py``: the mode -
        ``test_a_receiver_that_writes_no_rows_does_not_count``; ``eager`` -
        ``test_the_eager_attempt_counts_only_eager_receivers``; the lane -
        ``test_a_named_lane_counts_only_its_own_receivers``.
        """
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
        """Refuse a lane no receiver is declared in.

        A relay started for one - a typo in a manifest, or a lane renamed in
        the code and not in the deployment - would claim nothing, forever, and
        look healthy doing it. None is every lane, and the default lane exists
        with no receiver in it, because that is where a deleted receiver's rows
        drain.

        One arc, two conjuncts, each held by a case of
        ``test_a_declared_lane_the_default_and_every_lane_are_accepted``: None
        for the first, a declared lane for the second.
        """
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
