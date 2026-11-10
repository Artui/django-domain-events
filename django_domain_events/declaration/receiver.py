from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime, timedelta
from typing import Literal, TypeVar, overload

from django_domain_events.declaration.registry import registry
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import DEFAULT_LANE, RegisteredReceiver
from django_domain_events.utils import is_async_callable, label_for

E = TypeVar("E")


# A receiver may be ``async def``, so its return is None or an awaitable of it.
# Without the awaitable, a checker rejects every async receiver at the decorator.
Plain = Callable[[E], Awaitable[None] | None]
WithContext = Callable[[E, DeliveryContext], Awaitable[None] | None]
Targets = Callable[[E, DeliveryContext], Iterable[str]]
Due = Callable[[E], datetime]


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
    on_failure: Callable[[DeliveryFailure], Awaitable[None] | None] | None = None,
    targets: Targets[E] | None = None,
    backoff_base_seconds: float | None = None,
    backoff_cap_seconds: float | None = None,
    lane: str = DEFAULT_LANE,
    give_up_after: timedelta | None = None,
    due: timedelta | Due[E] | None = None,
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
    on_failure: Callable[[DeliveryFailure], Awaitable[None] | None] | None = None,
    targets: Targets[E] | None = None,
    backoff_base_seconds: float | None = None,
    backoff_cap_seconds: float | None = None,
    lane: str = DEFAULT_LANE,
    give_up_after: timedelta | None = None,
    due: timedelta | Due[E] | None = None,
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
    on_failure: Callable[[DeliveryFailure], Awaitable[None] | None] | None = None,
    targets: Targets[E] | None = None,
    backoff_base_seconds: float | None = None,
    backoff_cap_seconds: float | None = None,
    lane: str = DEFAULT_LANE,
    give_up_after: timedelta | None = None,
    due: timedelta | Due[E] | None = None,
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Register a callable to receive one event type, or every event."""

    if site not in ("relay", "task"):
        raise ValueError(f"site must be 'relay' or 'task', not {site!r}")
    if site == "task" and mode is not DeliveryMode.DURABLE:
        raise ValueError(
            f"site='task' needs mode=DURABLE; {mode.name} receivers run in the "
            f"firing process and have no delivery row to hand over."
        )
    if targets is not None and not callable(targets):
        raise TypeError(f"targets must be callable, got {type(targets).__name__}")
    for name, resolver in (("targets", targets), ("due", due)):
        if is_async_callable(resolver):
            raise TypeError(
                f"{name}= must be a plain function, not async def. It runs inside the "
                f"caller's transaction every time the event fires, so it is not run "
                f"on an event loop; read what it needs with the sync ORM."
            )
    if lease_seconds is not None and lease_seconds <= 0:
        raise ValueError(f"lease_seconds must be positive, got {lease_seconds}")
    for name, seconds in (
        ("backoff_base_seconds", backoff_base_seconds),
        ("backoff_cap_seconds", backoff_cap_seconds),
    ):
        if seconds is not None and seconds <= 0:
            raise ValueError(f"{name} must be positive, got {seconds}")
    if (
        backoff_base_seconds is not None
        and backoff_cap_seconds is not None
        and backoff_cap_seconds < backoff_base_seconds
    ):
        raise ValueError(
            f"backoff_cap_seconds={backoff_cap_seconds} is below "
            f"backoff_base_seconds={backoff_base_seconds}; every retry would wait "
            f"for the cap, so the base would never be read."
        )
    if not isinstance(lane, str) or not lane:
        raise ValueError(f"lane must be a non-empty string, got {lane!r}")
    if give_up_after is not None and not (
        isinstance(give_up_after, timedelta) and give_up_after > timedelta(0)
    ):
        raise ValueError(f"give_up_after must be a positive timedelta, got {give_up_after!r}")
    if due is not None and not (isinstance(due, timedelta) or callable(due)):
        raise TypeError(
            f"due must be a timedelta or a callable of the event returning a datetime, "
            f"got {type(due).__name__}"
        )
    if isinstance(due, timedelta) and due <= timedelta(0):
        raise ValueError(f"due must be a positive timedelta, got {due!r}")
    if due is not None and eager:
        raise ValueError(
            "due= cannot be combined with eager=True: an eager attempt runs at commit, "
            "and a scheduled delivery is not due then."
        )
    if due is not None and targets is not None:
        raise ValueError(
            "due= cannot be combined with targets=: a fan-out resolved when the event "
            "fires would keep that recipient set until the delivery comes due. Fire a "
            "follow-up event when it is due, and fan that out instead."
        )
    if mode is not DeliveryMode.DURABLE:
        for name, value, default in (
            ("max_attempts", max_attempts, 5),
            ("eager", eager, False),
            ("lease_seconds", lease_seconds, None),
            ("targets", targets, None),
            ("backoff_base_seconds", backoff_base_seconds, None),
            ("backoff_cap_seconds", backoff_cap_seconds, None),
            ("lane", lane, DEFAULT_LANE),
            ("give_up_after", give_up_after, None),
            ("due", due, None),
        ):
            if value != default:
                raise ValueError(
                    f"{name}={value!r} needs mode=DURABLE; a {mode.name} receiver "
                    f"has no delivery row, so it is never retried, never attempted "
                    f"a second time, never leased, never fanned out, never "
                    f"scheduled and never claimed by a relay."
                )

    def decorate(func: Callable[..., None]) -> Callable[..., None]:
        resolved_key = key if key is not None else _derived_key(func)
        registry.register_receiver(
            RegisteredReceiver(
                key=resolved_key,
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
                backoff_base_seconds=backoff_base_seconds,
                backoff_cap_seconds=backoff_cap_seconds,
                lane=lane,
                give_up_after=give_up_after,
                due=due,
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
