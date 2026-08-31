"""``@receiver`` - declare a callable as something that listens."""

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
) -> Callable[[Plain[E]], Plain[E]]: ...
@overload
def receiver(
    event_class: type[E],
    *,
    mode: DeliveryMode = DeliveryMode.DURABLE,
    takes_context: Literal[True],
    key: str | None = None,
    max_attempts: int = 5,
) -> Callable[[WithContext[E]], WithContext[E]]: ...
def receiver(
    event_class: type[E],
    *,
    mode: DeliveryMode = DeliveryMode.DURABLE,
    takes_context: bool = False,
    key: str | None = None,
    max_attempts: int = 5,
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Register a callable to receive one event type.

    ``takes_context`` is the spelling Django uses for the same idea in
    ``django.tasks.task``: an opt-in flag rather than inspecting the callable's
    parameters, because deciding what to pass by counting parameters is what
    makes a library feel haunted. The overloads above mean a checker enforces the
    arity the flag implies, so declaring one and writing the other is an error at
    the decorator rather than a ``TypeError`` in the relay three hours later.

        @receiver(OrderPlaced)
        def reserve_funds(evt: OrderPlaced) -> None: ...

        @receiver(OrderPlaced, takes_context=True, max_attempts=10)
        def notify(evt: OrderPlaced, ctx: DeliveryContext) -> None: ...

    ``max_attempts`` is copied onto each delivery row at fire time, so lowering
    it later does not retroactively dead-letter rows already in flight under the
    old limit.
    """

    def decorate(func: Callable[..., None]) -> Callable[..., None]:
        registry.register_receiver(
            RegisteredReceiver(
                key=key if key is not None else _derived_key(func),
                event_class=event_class,
                func=func,
                mode=mode,
                takes_context=takes_context,
                max_attempts=max_attempts,
            )
        )
        return func

    return decorate


def _derived_key(func: Callable[..., None]) -> str:
    """Build the default key from the declaring app and the callable's name.

    Refuses for a callable with no ``__name__`` -- a ``functools.partial``, or an
    instance with ``__call__``. There is no stable identity to derive there, and
    inventing one would write a key onto delivery rows that nothing can address
    later. ``key=`` is the answer, and the message says so.
    """
    name = getattr(func, "__name__", None)
    if name is None:
        raise TypeError(
            f"{func!r} has no __name__, so no stable receiver key can be derived "
            f"from it. Delivery rows address receivers by key, so pass an "
            f"explicit key=."
        )
    return label_for(func.__module__, name)
