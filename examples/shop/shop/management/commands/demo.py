"""One scenario, start to finish, printing what the log actually recorded."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.timezone import now

from django_domain_events import (
    assert_fired,
    attributed,
    deliver_pending,
    outbox_health,
    replay_events,
    requeue_dead,
    suppressed,
)
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from shop.events import OrderCancelled, OrderPlaced, ParcelDispatched
from shop.models import (
    Order,
    PartnerNotice,
    PartnerSubscription,
    Reservation,
    SentEmail,
    StockLevel,
)


def head(title: str) -> None:
    print(f"\n=== {title} ===")


def check(claim: str, actual: object, expected: object) -> None:
    """Assert what the line above just printed.

    Without this the demo is a script that prints sentences, and CI running it
    proves only that nothing raised. A receiver silently not firing, an eager
    attempt that stopped being eager, a suppression that started delivering -
    all of those would print the wrong number and exit 0.
    """
    if actual != expected:
        raise SystemExit(f"BROKEN: {claim}: expected {expected!r}, got {actual!r}")


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


class _Collect(logging.Handler):
    """Keeps the warnings the relay logs, so a step can check one was written."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class Command(BaseCommand):
    help = "Run the shop scenario against a real database."

    def handle(self, *args: Any, **options: Any) -> None:
        EventRecord.objects.all().delete()
        Order.objects.all().delete()
        SentEmail.objects.all().delete()
        PartnerSubscription.objects.all().delete()
        PartnerNotice.objects.all().delete()
        StockLevel.objects.update_or_create(sku="WIDGET", defaults={"available": 10})
        user, _ = User.objects.get_or_create(username="ana", defaults={"email": "ana@example.com"})

        head("1. An INLINE receiver vetoes a sale it cannot fill")
        try:
            place_order(user, "WIDGET", 99, 9900)
        except ValueError as exc:
            print(f"   refused: {exc}")
        print(f"   orders on record: {Order.objects.count()}")
        print(f"   events on record: {EventRecord.objects.count()}   <- the rollback took both")
        check("an INLINE veto rolls the order back", Order.objects.count(), 0)
        check("and takes the event with it", EventRecord.objects.count(), 0)

        head("2. A sale that goes through, attributed to whoever made it")
        with attributed(actor=user, source="checkout", channel="web"):
            order = place_order(user, "WIDGET", 3, 2999)
        print(f"   order {order.pk} placed")
        print(f"   emails sent immediately: {SentEmail.objects.count()}   <- eager=True")
        print(f"   reservations so far:     {Reservation.objects.count()}   <- still owed")
        row = EventRecord.objects.get(name="shop.OrderPlaced")
        print(f"   actor recorded: {row.actor_key} / {row.actor_label}  scope={row.scope}")
        check("eager=True sends the receipt at commit", SentEmail.objects.count(), 1)
        check("the durable work is still owed", Reservation.objects.count(), 0)
        check("the actor is on the row", row.actor_key, f"auth.User:{user.pk}")
        check("and so is the scope", row.scope, {"source": "checkout", "channel": "web"})

        head("3. The relay delivers what is owed")
        print(f"   {deliver_pending(worker_id='demo')}")
        print(f"   reservations now: {Reservation.objects.count()}")
        print(f"   stock left:       {StockLevel.objects.get(sku='WIDGET').available}")
        check("the relay reserves the stock", Reservation.objects.count(), 1)
        check("and decrements it once", StockLevel.objects.get(sku="WIDGET").available, 7)

        head("4. An event fired inside a receiver records what caused it")
        for name, pk, causation in EventRecord.objects.values_list("name", "pk", "causation_id"):
            print(f"   {name:22} id={pk} caused_by={causation}")
        follow_up = EventRecord.objects.get(name="shop.StockReserved")
        check("the follow-up names its parent", follow_up.causation_id, row.pk)

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
        check("the broken receiver dead-letters", dead.count(), 1)
        check("and only it", DeliveryRecord.objects.filter(status="dead").count(), 1)
        print(f"   requeued: {requeue_dead(receiver_key='shop.refund')}")
        check(
            "requeue resets the budget",
            DeliveryRecord.objects.get(receiver_key="shop.refund").attempts,
            0,
        )

        head("6. How far behind is the outbox?")
        health = outbox_health()
        print(f"   owed={health.owed} claimed={health.claimed} dead={health.dead}")
        print(f"   lapsed leases={health.lapsed_leases} oldest owed={health.oldest_owed_at}")
        for entry in health.receivers:
            print(f"     {entry.key:24} owed={entry.owed} dead={entry.dead}")
        check("a requeued row is owed again", health.owed, 1)
        check("and is no longer dead", health.dead, 0)
        if health.oldest_owed_at is None or health.oldest_owed_at > now():
            raise SystemExit("BROKEN: the oldest owed timestamp is in the future")

        head("7. A backfill that must be recorded but not delivered")
        with suppressed(OrderPlaced, reason="historical import"), transaction.atomic():
            legacy = Order.objects.create(customer=user, sku="WIDGET", quantity=1, total_cents=100)
            fire_order(legacy)
        suppressed_row = EventRecord.objects.get(suppressed_reason="historical import")
        print(f"   recorded id={suppressed_row.pk} reason={suppressed_row.suppressed_reason!r}")
        print(f"   deliveries owed for it: {suppressed_row.deliveries.count()}")
        check("a suppressed event is still recorded", bool(suppressed_row.pk), True)
        check("and owes nobody anything", suppressed_row.deliveries.count(), 0)

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
        check("the upgrade hook fills the field the row never had", rebuilt.currency, "EUR")

        head("9. A receiver that knows the destination is gone stops at once")
        with transaction.atomic():
            from django_domain_events import fire

            fire(ParcelDispatched(order_id=order.pk, carrier="parcelforce"))
        warnings = _Collect()
        relay_log = logging.getLogger("django_domain_events.delivery.deliver")
        relay_log.addHandler(warnings)
        before = now()
        try:
            print(f"   {deliver_pending(worker_id='demo')}")
        finally:
            relay_log.removeHandler(warnings)
        after = now()
        gone = DeliveryRecord.objects.get(receiver_key="shop.notify_marketplace")
        print(f"   {gone.receiver_key}: {gone.status} after {gone.attempts} of {gone.max_attempts}")
        print(f"         {gone.last_error}")
        check("PermanentFailure dead-letters on the attempt that raised it", gone.status, "dead")
        check("having spent one attempt of five", (gone.attempts, gone.max_attempts), (1, 5))

        head("10. A receiver told when to come back is retried then, within a ceiling")
        limited = DeliveryRecord.objects.get(receiver_key="shop.register_tracking")
        wait = limited.available_at - before
        print(
            f"   {limited.receiver_key}: {limited.status}, next attempt in {wait.total_seconds():.0f}s"
        )
        print(f"         {limited.last_error}")
        check("RetryAfter counts the attempt", (limited.status, limited.attempts), ("failed", 1))
        check(
            "and schedules the next one when the carrier asked",
            before + timedelta(seconds=120)
            <= limited.available_at
            <= after + timedelta(seconds=120),
            True,
        )
        closed = DeliveryRecord.objects.get(receiver_key="shop.book_customs_clearance")
        parked = closed.available_at - before
        print(
            f"   {closed.receiver_key}: asked for 2 days, parked for {parked.total_seconds():.0f}s"
        )
        for message in warnings.messages:
            print(f"         warned: {message}")
        check(
            "a request past MAX_RECEIVER_RETRY_DELAY_SECONDS is clamped to it",
            before + timedelta(days=1) <= closed.available_at <= after + timedelta(days=1),
            True,
        )
        check(
            "and the clamp is logged",
            [m for m in warnings.messages if "MAX_RECEIVER_RETRY_DELAY_SECONDS" in m] != [],
            True,
        )

        head("11. One receiver for every event, one delivery per partner that wants it")
        forward = "shop.forward_to_partners"
        PartnerSubscription.objects.create(partner="acme-analytics", event_name="shop.OrderPlaced")
        PartnerSubscription.objects.create(partner="globex-crm", event_name="shop.OrderPlaced")
        second = place_order(user, "WIDGET", 1, 999)
        placed = EventRecord.objects.filter(name="shop.OrderPlaced").latest("pk")
        rows = DeliveryRecord.objects.filter(event=placed, receiver_key=forward).order_by("target")
        print(f"   order {second.pk}: delivery rows for {forward}: {[r.target for r in rows]}")
        check(
            "an AnyEvent receiver gets one row per target",
            [r.target for r in rows],
            ["acme-analytics", "globex-crm"],
        )
        deliver_pending(worker_id="demo")
        reserved = EventRecord.objects.filter(name="shop.StockReserved").latest("pk")
        unwanted = DeliveryRecord.objects.filter(event=reserved, receiver_key=forward).count()
        print(f"   {reserved.name} (no partner wants it): {unwanted} rows")
        notices = sorted(PartnerNotice.objects.values_list("partner", flat=True))
        print(f"   partners sent it: {notices}")
        check("an event whose targets are empty writes no row", unwanted, 0)
        check("each partner is sent it once", notices, ["acme-analytics", "globex-crm"])

        head("12. A replay asks for the targets again")
        PartnerSubscription.objects.filter(partner="globex-crm").delete()
        PartnerSubscription.objects.create(partner="initech-erp", event_name="shop.OrderPlaced")
        counts = replay_events([placed.pk], receiver_keys=[forward])
        print("   globex-crm unsubscribed, initech-erp subscribed, then replayed:")
        print(f"   {counts}")
        check(
            "acme still wants it and is reopened, initech is added",
            counts,
            {"reopened": 1, "added": 1},
        )
        globex = DeliveryRecord.objects.get(event=placed, receiver_key=forward, target="globex-crm")
        print(f"   globex-crm's delivery: {globex.status} after {globex.attempts} attempt")
        check(
            "a target no longer returned is left as it was",
            (globex.status, globex.attempts),
            ("succeeded", 1),
        )
        deliver_pending(worker_id="demo")
        notices = sorted(PartnerNotice.objects.values_list("partner", flat=True))
        print(f"   partners sent it, all told: {notices}")
        check(
            "the replay reaches the partners subscribed now",
            notices,
            ["acme-analytics", "acme-analytics", "globex-crm", "initech-erp"],
        )
        print("\nEvery claim above was checked.")
