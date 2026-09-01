"""Every declaration this app makes, in one module the app config autodiscovers.

Written as a worked example: each receiver below exists to show one knob doing
something a real application would want.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.db.models import F

from django_domain_events import (
    DURABLE,
    INLINE,
    ON_COMMIT,
    DeliveryContext,
    event,
    fire,
    receiver,
)


@event(name="shop.OrderPlaced", version=2)
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    """Someone bought something. Version 2 added `currency`, with no default.

    A v1 row therefore cannot be decoded by the constructor alone, which is
    exactly the case `upgrade` exists for. Without it every row written before
    the deploy dead-letters, one attempt budget at a time.
    """

    order_id: int
    sku: str
    quantity: int
    total_cents: int
    currency: str

    @staticmethod
    def upgrade(payload: dict[str, Any], from_version: int) -> dict[str, Any]:
        # The shop only ever sold in euros before v2 introduced the column.
        return {**payload, "currency": "EUR"}


@event(name="shop.OrderCancelled")
@dataclass(frozen=True, slots=True)
class OrderCancelled:
    order_id: int
    reason: str


@event(name="shop.StockReserved")
@dataclass(frozen=True, slots=True)
class StockReserved:
    order_id: int
    sku: str
    quantity: int


@receiver(OrderPlaced, mode=INLINE)
def refuse_orders_we_cannot_fill(evt: OrderPlaced) -> None:
    """INLINE, because this one is allowed to veto the sale.

    It runs inside the firing transaction, so raising here rolls the order back
    with it - and nothing is owed, because nothing committed. That is the only
    mode where a receiver may abort the caller's work, and the reason it needs
    no durability of its own.
    """
    from shop.models import StockLevel

    level = StockLevel.objects.select_for_update().get(sku=evt.sku)
    if level.available < evt.quantity:
        raise ValueError(f"only {level.available} of {evt.sku} left, wanted {evt.quantity}")


@receiver(OrderPlaced, mode=DURABLE, key="shop.reserve_stock")
def reserve_stock(evt: OrderPlaced) -> None:
    """DURABLE and touching only this database, so it is effectively once.

    Its write and its acknowledgement commit together: the duplicate that
    at-least-once entitles you to cannot be observed here. It also fires a
    follow-up event, which the relay automatically records as caused by this
    one - no plumbing at the call site.
    """
    from shop.models import Order, Reservation, StockLevel

    order = Order.objects.get(pk=evt.order_id)
    Reservation.objects.get_or_create(order=order, defaults={"quantity": evt.quantity})
    StockLevel.objects.filter(sku=evt.sku).update(available=F("available") - evt.quantity)
    fire(StockReserved(order_id=evt.order_id, sku=evt.sku, quantity=evt.quantity))


@receiver(OrderPlaced, mode=DURABLE, eager=True, key="shop.email_receipt", max_attempts=8)
def email_receipt(evt: OrderPlaced) -> None:
    """A side effect the database cannot undo, so at-least-once is real here.

    `eager=True` attempts it the moment the transaction commits, in the web
    process, with the relay as the fallback - outbox durability at on-commit
    latency. `max_attempts=8` because a mail provider being down for an hour is
    ordinary and the default five would dead-letter through it.
    """
    from shop.models import Order, SentEmail

    order = Order.objects.get(pk=evt.order_id)
    SentEmail.objects.create(
        to=order.customer.email or "nobody@example.com",
        subject=f"Your order of {evt.quantity} x {evt.sku}",
    )


@receiver(OrderPlaced, mode=DURABLE, takes_context=True, key="shop.audit", lease_seconds=900)
def write_audit_trail(evt: OrderPlaced, ctx: DeliveryContext) -> None:
    """`takes_context` for the attribution the event row carries.

    The actor and scope come off the row, not off a ContextVar, so this reads
    correctly hours later in the relay process. `lease_seconds=900` because
    this one talks to a slow warehouse system in the real version, and a
    receiver that outruns its lease has its work thrown away.
    """
    print(
        f"      audit: {ctx.event_name} by {ctx.actor_key} scope={ctx.scope} attempt={ctx.attempt}"
    )


@receiver(OrderPlaced, mode=ON_COMMIT, key="shop.warm_cache")
def warm_cache(evt: OrderPlaced) -> None:
    """ON_COMMIT: best effort, no row, no retry.

    Right for work that is pure optimisation. If the process dies here the
    cache is simply cold, and paying for a delivery row to guarantee a cache
    warm would be the wrong trade.
    """


@receiver(StockReserved, mode=DURABLE, key="shop.notify_warehouse")
def notify_warehouse(evt: StockReserved) -> None:
    """Second hop. Its event was fired inside a receiver, so the log records
    which order caused it without anyone passing an id around."""


@receiver(OrderCancelled, mode=DURABLE, key="shop.release_stock")
def release_stock(evt: OrderCancelled) -> None:
    from shop.models import Order, Reservation, StockLevel

    order = Order.objects.get(pk=evt.order_id)
    reservation = Reservation.objects.filter(order=order).first()
    if reservation is not None:
        StockLevel.objects.filter(sku=order.sku).update(
            available=F("available") + reservation.quantity
        )
        reservation.delete()


@receiver(OrderCancelled, mode=DURABLE, key="shop.refund")
def refund(evt: OrderCancelled) -> None:
    """Deliberately broken, to show the dead-letter path and the requeue."""
    raise RuntimeError("payment gateway timed out")
