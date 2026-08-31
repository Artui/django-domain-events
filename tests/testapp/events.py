"""Declarations the autodiscovery picks up, exercising the real entry point.

Deliberately declared here rather than inside each test: autodiscovery of an
``events`` module is how a consumer's declarations actually load, and a suite
that only ever registers inline never runs that path.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Literal
from uuid import UUID

from django_domain_events import DURABLE, INLINE, ON_COMMIT, DeliveryContext, event, receiver


class Currency(str, Enum):
    EUR = "EUR"
    USD = "USD"


@event
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    """Every scalar the default codec claims, so the claim is tested."""

    order_id: int
    total: Decimal
    placed_at: datetime
    trace: UUID
    currency: Currency
    kind: Literal["retail", "wholesale"]
    tags: list[str]
    note: str | None = None


@event(name="testapp.pinned", version=3)
@dataclass(frozen=True, slots=True)
class PinnedName:
    value: int


@event
@dataclass(frozen=True, slots=True)
class Unheard:
    """Declared, never listened to. The registry should still know it."""

    value: int


calls: list[str] = []
"""What ran, in order. Reset by the ``record`` fixture."""


@receiver(OrderPlaced, mode=DURABLE)
def durable_receiver(evt: OrderPlaced) -> None:
    calls.append(f"durable:{evt.order_id}")


@receiver(OrderPlaced, mode=DURABLE, takes_context=True, key="testapp.with_context")
def context_receiver(evt: OrderPlaced, ctx: DeliveryContext) -> None:
    calls.append(f"context:{ctx.event_name}:{ctx.attempt}")


@receiver(OrderPlaced, mode=INLINE)
def inline_receiver(evt: OrderPlaced) -> None:
    calls.append(f"inline:{evt.order_id}")


@receiver(OrderPlaced, mode=ON_COMMIT)
def on_commit_receiver(evt: OrderPlaced) -> None:
    calls.append(f"on_commit:{evt.order_id}")


@receiver(OrderPlaced, mode=INLINE, takes_context=True, key="testapp.inline_with_context")
def inline_context_receiver(evt: OrderPlaced, ctx: DeliveryContext) -> None:
    """An inline receiver that wants the context.

    Its attempt is always 1 and it has no delivery row, which is exactly what
    the mode promises: it cannot be retried, because a failure rolls the whole
    transaction back instead.
    """
    calls.append(f"inline_context:{ctx.event_name}:{ctx.attempt}")


@event
@dataclass(frozen=True, slots=True)
class Eagerly:
    """Its own event, so the eager fan-out does not perturb OrderPlaced's."""

    value: int


@receiver(Eagerly, mode=DURABLE, eager=True, key="testapp.eager")
def eager_receiver(evt: Eagerly) -> None:
    calls.append(f"eager:{evt.value}")


@receiver(Eagerly, mode=DURABLE, key="testapp.not_eager")
def not_eager_receiver(evt: Eagerly) -> None:
    calls.append(f"not_eager:{evt.value}")
