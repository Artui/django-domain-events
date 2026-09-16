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
    AnyEvent,
    DeliveryContext,
    PermanentFailure,
    RetryAfter,
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


@event(name="shop.ParcelDispatched")
@dataclass(frozen=True, slots=True)
class ParcelDispatched:
    """The parcel left the warehouse, and three outside systems want to know."""

    order_id: int
    carrier: str


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


@receiver(ParcelDispatched, mode=DURABLE, key="shop.notify_marketplace")
def notify_marketplace(evt: ParcelDispatched) -> None:
    """`PermanentFailure`, because the other side has said it will never accept this.

    The marketplace answered `410 Gone`: it de-listed the shop. Raising an
    ordinary exception would spend the whole attempt budget - five POSTs across
    the next hour to a URL that has already said no. Raising this dead-letters
    the delivery on the attempt that learned it. Deliberately always gone, so
    the demo can show it.
    """
    raise PermanentFailure("410 Gone: the marketplace de-listed this shop")


@receiver(ParcelDispatched, mode=DURABLE, key="shop.register_tracking")
def register_tracking(evt: ParcelDispatched) -> None:
    """`RetryAfter`, because the carrier said exactly when to come back.

    Its tracking API answered `429` with `Retry-After: 120`. The backoff curve
    would guess, arrive early and be refused again; this schedules the next
    attempt for when the carrier asked. It still counts as an attempt, so a
    carrier that rate-limits forever still dead-letters within the budget.
    Deliberately always rate limited, so the demo can show it.
    """
    raise RetryAfter(seconds=120, reason="the carrier answered 429 with Retry-After: 120")


@receiver(ParcelDispatched, mode=DURABLE, key="shop.book_customs_clearance")
def book_customs_clearance(evt: ParcelDispatched) -> None:
    """`RetryAfter` past the ceiling, because a destination's number is advice.

    The customs broker is closed for a two-day holiday and says so. A delivery
    parked in the future is still owed, which keeps its event past retention,
    so the relay clamps the request to `MAX_RECEIVER_RETRY_DELAY_SECONDS` - a
    day by default - and logs a warning naming both numbers.
    """
    raise RetryAfter(seconds=2 * 86400, reason="the broker is closed for a two-day holiday")


def partners_subscribed(evt: object, ctx: DeliveryContext) -> list[str]:
    """The partners that want this event, read from the table they are kept in.

    Called by `fire()` for every event the shop fires, inside the transaction
    that fired it, so it is one indexed query in every write this app makes - and
    if it raised, the write would roll back with it rather than deliver to
    nobody. An event no partner subscribes to returns nothing, and nothing
    returned means no delivery row at all.
    """
    from shop.models import PartnerSubscription

    return list(
        PartnerSubscription.objects.filter(event_name=ctx.event_name)
        .order_by("partner")
        .values_list("partner", flat=True)
    )


@receiver(
    AnyEvent,
    mode=DURABLE,
    takes_context=True,
    key="shop.forward_to_partners",
    targets=partners_subscribed,
)
def forward_to_partners(evt: object, ctx: DeliveryContext) -> None:
    """`AnyEvent` with `targets=`: a transport, owed every event, once per partner.

    Declared once for every event rather than once per event class, so an event
    added next year - by an app installed after this one - is forwarded without
    anyone remembering to. Each partner gets a delivery row of its own, with its
    own attempts and its own dead-letter, and learns which one it is from
    `ctx.target`. A replay asks `partners_subscribed` again, so it goes to the
    partners subscribed at replay time.
    """
    from shop.models import PartnerNotice

    PartnerNotice.objects.create(
        partner=ctx.target, event_name=ctx.event_name, event_id=ctx.event_id
    )
