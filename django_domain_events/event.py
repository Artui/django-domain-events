"""``@event`` - declare a class as something that can be fired."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar, overload

from django_domain_events.registry import registry
from django_domain_events.types.registered_event import RegisteredEvent
from django_domain_events.utils import label_for, require_frozen_dataclass

E = TypeVar("E", bound=type)


@overload
def event(cls: E) -> E: ...
@overload
def event(*, name: str | None = None, version: int = 1) -> Callable[[E], E]: ...
def event(
    cls: type | None = None, *, name: str | None = None, version: int = 1
) -> type | Callable[[type], type]:
    """Register a frozen dataclass as an event.

    Usable bare or called::

        @event
        @dataclass(frozen=True, slots=True)
        class OrderPlaced:
            order_id: int

        @event(name="orders.placed", version=2)
        @dataclass(frozen=True, slots=True)
        class OrderPlacedV2:
            order_id: int

    The default name is ``<app_label>.<ClassName>``. Pin it with ``name=`` when
    renaming the class would otherwise strand rows already written under the old
    one: the name is what a delivery row carries, not the class.
    """

    def decorate(target: type) -> type:
        require_frozen_dataclass(target)
        resolved = name if name is not None else label_for(target.__module__, target.__name__)
        registry.register_event(RegisteredEvent(event_class=target, name=resolved, version=version))
        return target

    if cls is not None:
        return decorate(cls)
    return decorate
