from __future__ import annotations

from collections.abc import Callable
from typing import Literal, TypeVar, overload

from django_domain_events.registry import registry
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from django_domain_events.utils import label_for

E = TypeVar("E")

Plain = Callable[[E], None]
WithContext = Callable[[E, DeliveryContext], None]


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
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Register a callable to receive one event type.

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

    ``lease_seconds`` overrides ``LEASE_SECONDS`` for this receiver alone, and
    is the answer for one that legitimately runs long. A receiver still working
    when its lease lapses has its row taken by another worker and its own work
    rolled back - correct, and entirely wasted. Declaring it here rather than
    offering the receiver a way to extend its lease from the inside, because
    that cannot work: the receiver runs inside the transaction that carries its
    acknowledgement, so anything it writes is invisible to every other worker
    until it has already finished.
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
        ):
            if value != default:
                raise ValueError(
                    f"{name}={value!r} needs mode=DURABLE; a {mode.name} receiver "
                    f"has no delivery row, so it is never retried, never attempted "
                    f"a second time and never leased."
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
                lease_seconds=lease_seconds,
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
