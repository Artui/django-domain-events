from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Literal, TypeVar, overload

from django_domain_events.declaration.registry import registry
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import label_for

E = TypeVar("E")

Plain = Callable[[E], None]
WithContext = Callable[[E, DeliveryContext], None]
Targets = Callable[[E, DeliveryContext], Iterable[str]]


@overload
def receiver(
    event_class: type[E],
    *,
    mode: DeliveryMode = DeliveryMode.DURABLE,
    takes_context: Literal[False] = False,
    key: str | None = None,
    max_attempts: int = 5,
    eager: bool = False,
    site: str = "relay",
    lease_seconds: int | None = None,
    on_failure: Callable[[DeliveryFailure], None] | None = None,
    targets: Targets[E] | None = None,
) -> Callable[[Plain[E]], Plain[E]]: ...
@overload
def receiver(
    event_class: type[E],
    *,
    mode: DeliveryMode = DeliveryMode.DURABLE,
    takes_context: Literal[True],
    key: str | None = None,
    max_attempts: int = 5,
    eager: bool = False,
    site: str = "relay",
    lease_seconds: int | None = None,
    on_failure: Callable[[DeliveryFailure], None] | None = None,
    targets: Targets[E] | None = None,
) -> Callable[[WithContext[E]], WithContext[E]]: ...
def receiver(
    event_class: type[E],
    *,
    mode: DeliveryMode = DeliveryMode.DURABLE,
    takes_context: bool = False,
    key: str | None = None,
    max_attempts: int = 5,
    eager: bool = False,
    site: str = "relay",
    lease_seconds: int | None = None,
    on_failure: Callable[[DeliveryFailure], None] | None = None,
    targets: Targets[E] | None = None,
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Register a callable to receive one event type, or every event.

    ``event_class`` may be ``AnyEvent``, which declares a wildcard: the receiver
    is owed every event fired, including events declared by apps that load
    after it. See ``AnyEvent`` for what that is for and what it costs.

    ``takes_context`` is the spelling ``django.tasks.task`` uses for the same
    idea. The overloads make a checker enforce the arity it implies, so
    declaring one and writing the other fails at the decorator rather than in
    the relay hours later.

    ``max_attempts`` is copied onto each delivery row at fire time.

    ``site`` is the execution knob, separate from ``mode`` on purpose: timing is
    what a receiver promises about the transaction, and where its code runs is a
    different question that only a queue answers. ``"relay"`` runs it in the
    relay worker; ``"task"`` hands it to the configured task backend, which then
    acknowledges the row when it finishes.

    ``eager`` additionally attempts delivery immediately after commit, in the
    firing process, with the relay as the fallback for whatever process death
    loses. It is what stops ``DURABLE`` feeling slow: outbox durability at
    on-commit latency, at the cost of a duplicate when the process dies
    mid-receiver - which at-least-once already required everyone to tolerate.

    ``on_failure`` is called after a failed attempt has been recorded, outside
    the transaction that was just rolled back, so a receiver can keep a durable
    record of its own failure. It could not before: everything a receiver writes
    commits with its acknowledgement and is discarded the moment it raises, so
    the attempts worth logging were exactly the ones that could not be logged.
    The hook must not raise; one that does is logged and swallowed, because a
    failure path that fails is worse than a lost log line.

    ``lease_seconds`` overrides ``LEASE_SECONDS`` for this receiver alone, and
    is the answer for one that legitimately runs long. A receiver still working
    when its lease lapses has its row taken by another worker and its own work
    rolled back - correct, and entirely wasted. Declaring it here rather than
    offering the receiver a way to extend its lease from the inside, because
    that cannot work: the receiver runs inside the transaction that carries its
    acknowledgement, so anything it writes is invisible to every other worker
    until it has already finished.

    ``targets`` makes the receiver a **fan-out**: a callable taking the event and
    a ``DeliveryContext``, returning the strings this event is owed to - endpoint
    ids, tenant slugs, anything that names a destination. ``fire()`` writes one
    delivery row per target instead of one per receiver, each with its own
    attempt count, backoff and dead-letter, and hands the target to the receiver
    as ``DeliveryContext.target``. A target returned twice is delivered once,
    and a callable returning nothing writes no row at all, which is how a
    wildcard receiver says "not this event". Each target must be a non-empty
    string of at most 255 characters; anything else is refused where it is
    returned.

    **The callable runs inside the caller's transaction, at fire time**, exactly
    as the event row is written - so ``fire()``'s transactional contract is its
    contract too. What it reads is what the business change can see, including
    that change's own uncommitted rows. **If it raises, ``fire()`` raises**, and
    the caller's transaction fails with it: the change, the event and every
    delivery row roll back together. That is deliberate. A fan-out that swallowed
    the error and delivered to nobody would commit a change whose consequences
    silently never happened, which is the failure this package exists to rule
    out. Keep the callable's failure modes in view: it is code in the middle of
    somebody else's write.

    **It is also a query in the hot path.** ``fire()`` calls every fan-out
    receiver's callable on every event it is owed - for a wildcard, every event
    fired - so a callable that queries a table costs one query per fired event
    per fan-out receiver. For a transport replacing an event per destination
    that is a clear saving; for one declared carelessly it is a cost paid by
    every write in the system. Make the lookup indexed, and make it return
    early for events it can rule out without one.

    ``replay_events`` calls it again, so a replay goes to the targets that exist
    at replay time.
    """

    if site not in ("relay", "task"):
        raise ValueError(f"site must be 'relay' or 'task', not {site!r}")
    if site == "task" and mode is not DeliveryMode.DURABLE:
        # INLINE and ON_COMMIT have no delivery row, so there is nothing to hand
        # to a backend. Accepting the combination would run the receiver in the
        # firing process while the declaration says otherwise.
        raise ValueError(
            f"site='task' needs mode=DURABLE; {mode.name} receivers run in the "
            f"firing process and have no delivery row to hand over."
        )
    if targets is not None and not callable(targets):
        # Refused here rather than at the first fire, which would raise inside
        # somebody's business transaction for a declaration mistake.
        raise TypeError(f"targets must be callable, got {type(targets).__name__}")
    if lease_seconds is not None and lease_seconds <= 0:
        # A zero lease expires the instant before the receiver starts, so a
        # second relay reclaims the row immediately and both run it - the exact
        # double delivery the lease exists to prevent.
        raise ValueError(f"lease_seconds must be positive, got {lease_seconds}")
    if mode is not DeliveryMode.DURABLE:
        # Same reasoning as site=, applied to the rest of the row-shaped knobs.
        # Accepting them would let a declaration state a retry budget, an eager
        # attempt or a lease for a receiver that has no row to carry any of
        # them, and nothing downstream would ever say so - the catalogue would
        # publish the numbers and the relay would ignore them.
        for name, value, default in (
            ("max_attempts", max_attempts, 5),
            ("eager", eager, False),
            ("lease_seconds", lease_seconds, None),
            ("targets", targets, None),
        ):
            if value != default:
                raise ValueError(
                    f"{name}={value!r} needs mode=DURABLE; a {mode.name} receiver "
                    f"has no delivery row, so it is never retried, never attempted "
                    f"a second time, never leased and never fanned out."
                )

    def decorate(func: Callable[..., None]) -> Callable[..., None]:
        registry.register_receiver(
            RegisteredReceiver(
                key=key if key is not None else _derived_key(func),
                event_class=event_class,
                func=func,
                mode=mode,
                takes_context=takes_context,
                max_attempts=max_attempts,
                eager=eager,
                site=site,
                on_failure=on_failure,
                lease_seconds=lease_seconds,
                targets=targets,
            )
        )
        return func

    return decorate


def _derived_key(func: Callable[..., None]) -> str:
    """Build the default key from the declaring app and the callable's name."""
    name = getattr(func, "__name__", None)
    if name is None:
        raise TypeError(
            f"{func!r} has no __name__, so no stable receiver key can be derived "
            f"from it. Delivery rows address receivers by key, so pass key=."
        )
    return label_for(func.__module__, name)
