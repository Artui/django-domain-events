"""One scenario, start to finish, printing what the log actually recorded."""

from typing import Any

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.db import transaction

from django_domain_events import (
    assert_fired,
    attributed,
    deliver_pending,
    outbox_health,
    requeue_dead,
    suppressed,
)
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from shop.events import OrderCancelled, OrderPlaced
from shop.models import Order, Reservation, SentEmail, StockLevel


def head(title: str) -> None:
    print(f"\n=== {title} ===")


def place_order(user: User, sku: str, quantity: int, cents: int) -> Order:
    """The shape every write in this app takes.

    One transaction. The row and the event are written together, so the event
    exists if and only if the order does.
    """
    with transaction.atomic():
        order = Order.objects.create(customer=user, sku=sku, quantity=quantity, total_cents=cents)
        fire_order(order)
        return order


def fire_order(order: Order) -> None:
    from django_domain_events import fire

    fire(
        OrderPlaced(
            order_id=order.pk,
            sku=order.sku,
            quantity=order.quantity,
            total_cents=order.total_cents,
            currency="EUR",
        ),
        dedupe_key=f"order-placed:{order.pk}",
    )


class Command(BaseCommand):
    help = "Run the shop scenario against a real database."

    def handle(self, *args: Any, **options: Any) -> None:
        EventRecord.objects.all().delete()
        Order.objects.all().delete()
        SentEmail.objects.all().delete()
        StockLevel.objects.update_or_create(sku="WIDGET", defaults={"available": 10})
        user, _ = User.objects.get_or_create(username="ana", defaults={"email": "ana@example.com"})

        head("1. An INLINE receiver vetoes a sale it cannot fill")
        try:
            place_order(user, "WIDGET", 99, 9900)
        except ValueError as exc:
            print(f"   refused: {exc}")
        print(f"   orders on record: {Order.objects.count()}")
        print(f"   events on record: {EventRecord.objects.count()}   <- the rollback took both")

        head("2. A sale that goes through, attributed to whoever made it")
        with attributed(actor=user, source="checkout", channel="web"):
            order = place_order(user, "WIDGET", 3, 2999)
        print(f"   order {order.pk} placed")
        print(f"   emails sent immediately: {SentEmail.objects.count()}   <- eager=True")
        print(f"   reservations so far:     {Reservation.objects.count()}   <- still owed")
        row = EventRecord.objects.get(name="shop.OrderPlaced")
        print(f"   actor recorded: {row.actor_key} / {row.actor_label}  scope={row.scope}")

        head("3. The relay delivers what is owed")
        print(f"   {deliver_pending(worker_id='demo')}")
        print(f"   reservations now: {Reservation.objects.count()}")
        print(f"   stock left:       {StockLevel.objects.get(sku='WIDGET').available}")

        head("4. An event fired inside a receiver records what caused it")
        for name, pk, causation in EventRecord.objects.values_list("name", "pk", "causation_id"):
            print(f"   {name:22} id={pk} caused_by={causation}")

        head("5. A receiver that keeps failing dead-letters, and can be requeued")
        with transaction.atomic():
            order.cancelled = True
            order.save(update_fields=["cancelled"])
            from django_domain_events import fire

            fire(OrderCancelled(order_id=order.pk, reason="customer changed their mind"))
        for _ in range(6):
            deliver_pending(worker_id="demo", ignore_backoff=True)
        dead = DeliveryRecord.objects.filter(status="dead")
        for entry in dead:
            print(f"   dead: {entry.receiver_key} after {entry.attempts} attempts")
            print(f"         {entry.last_error}")
        print(f"   requeued: {requeue_dead(receiver_key='shop.refund')}")

        head("6. How far behind is the outbox?")
        health = outbox_health()
        print(f"   owed={health.owed} claimed={health.claimed} dead={health.dead}")
        print(f"   lapsed leases={health.lapsed_leases} oldest owed={health.oldest_owed_at}")
        for entry in health.receivers:
            print(f"     {entry.key:24} owed={entry.owed} dead={entry.dead}")

        head("7. A backfill that must be recorded but not delivered")
        with suppressed(OrderPlaced, reason="historical import"), transaction.atomic():
            legacy = Order.objects.create(customer=user, sku="WIDGET", quantity=1, total_cents=100)
            fire_order(legacy)
        suppressed_row = EventRecord.objects.get(suppressed_reason="historical import")
        print(f"   recorded id={suppressed_row.pk} reason={suppressed_row.suppressed_reason!r}")
        print(f"   deliveries owed for it: {suppressed_row.deliveries.count()}")

        head("8. A row written before the class gained a required field")
        EventRecord.objects.filter(pk=row.pk).update(
            version=1,
            payload={
                "order_id": order.pk,
                "sku": "WIDGET",
                "quantity": 3,
                "total_cents": 2999,
            },
        )
        rebuilt = assert_fired(OrderPlaced)[0]
        print(f"   decoded a v1 row as: {rebuilt}")
        print("   currency came from OrderPlaced.upgrade()")
