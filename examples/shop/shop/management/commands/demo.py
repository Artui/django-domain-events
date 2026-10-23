"""One scenario, start to finish, printing what the log actually recorded."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from io import StringIO
from typing import Any

from django.conf import settings as django_settings
from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.test import override_settings
from django.utils.timezone import now

from django_domain_events import (
    DURABLE,
    assert_fired,
    attributed,
    backoff,
    cancel_deliveries,
    catalogue,
    claim_batch,
    deliver_pending,
    drain_outbox,
    event,
    outbox_health,
    prune_events,
    quiet_receivers,
    receiver,
    registry,
    replay_events,
    requeue_dead,
    suppressed,
)
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.models.receiver_last_success import ReceiverLastSuccess
from shop.delivery_log import DeliveryLogFilter, current_delivery
from shop.events import (
    REMINDER_QUIET_PERIOD,
    InvoiceSent,
    OrderCancelled,
    OrderDelivered,
    OrderPlaced,
    ParcelDispatched,
)
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
    """Keeps the warnings the relay logs, so a step can check one was written.

    With the delivery each was stamped with by the AROUND_DELIVERY hook, so a
    step can check the relay's own lines carry it too.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.addFilter(DeliveryLogFilter())
        self.messages: list[str] = []
        self.stamps: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())
        self.stamps.append(getattr(record, "delivery", ""))


class _Lines(logging.Handler):
    """Keeps each line with the delivery the hook stamped it with."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.addFilter(DeliveryLogFilter())
        self.lines: list[tuple[str, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append((getattr(record, "delivery", ""), record.getMessage()))


class Command(BaseCommand):
    help = "Run the shop scenario against a real database."

    def handle(self, *args: Any, **options: Any) -> None:
        EventRecord.objects.all().delete()
        # Else a second run inherits the first run's record of who succeeded,
        # and step 13 would pass whether or not its own prune wrote one.
        ReceiverLastSuccess.objects.all().delete()
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
        # A relay claimed the requeued row and stopped before finishing it, so
        # the claimed counts have something to say: with nothing claimed, every
        # receiver's zero would add up to a zero total whether or not it was
        # counted. Its lease has already run out, which is also what lets the
        # drains in the later steps take the row again.
        claim_batch(
            worker_id="demo-relay",
            now=now(),
            lease=timedelta(0),
            limit=1,
            ignore_backoff=True,
        )
        health = outbox_health()
        print(f"   owed={health.owed} claimed={health.claimed} dead={health.dead}")
        print(f"   lapsed leases={health.lapsed_leases} oldest owed={health.oldest_owed_at}")
        for entry in health.receivers:
            print(
                f"     {entry.key:24} owed={entry.owed} dead={entry.dead} claimed={entry.claimed}"
            )
        check("a requeued row is owed again", health.owed, 1)
        check("a relay claimed it", health.claimed, 1)
        check("and its lease has lapsed", health.lapsed_leases, 1)
        check(
            "and the receiver it belongs to says so",
            [entry.key for entry in health.receivers if entry.claimed],
            ["shop.refund"],
        )
        check(
            "each receiver's claimed rows add up to the total",
            sum(entry.claimed for entry in health.receivers),
            health.claimed,
        )
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
        check(
            "the relay's warning for it carries the ids the AROUND_DELIVERY hook set",
            [
                stamp
                for stamp, message in zip(warnings.stamps, warnings.messages, strict=True)
                if f"failed delivery {gone.pk} " in message
            ],
            [f"delivery={gone.pk} receiver=shop.notify_marketplace"],
        )

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
        quota = DeliveryRecord.objects.get(receiver_key="shop.email_dispatch_notice")
        deferred = quota.available_at - before
        print(
            f"   {quota.receiver_key}: {quota.status} after {quota.attempts} attempts, "
            f"next in {deferred.total_seconds():.0f}s"
        )
        print(f"         {quota.last_error}")
        check(
            "RetryAfter(counts=False) spends no attempt",
            (quota.status, quota.attempts),
            ("failed", 0),
        )
        check(
            "and comes back between the hour asked and twice that",
            before + timedelta(hours=1) <= quota.available_at <= after + timedelta(hours=2),
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

        head("13. An event declared to go once consumed goes at the next prune")
        deliver_pending(worker_id="demo")
        reservations = EventRecord.objects.filter(name="shop.StockReserved")
        orders_logged = EventRecord.objects.filter(name="shop.OrderPlaced").count()
        print(f"   StockReserved events delivered and waiting: {reservations.count()}")
        check(
            "each was delivered to every receiver",
            set(
                DeliveryRecord.objects.filter(event__in=reservations).values_list(
                    "status", flat=True
                )
            ),
            {"succeeded"},
        )
        check("both reservations are still on record", reservations.count(), 2)
        print(f"   pruned: {prune_events()} events")
        print(f"   StockReserved events left: {reservations.count()}")
        orders_left = EventRecord.objects.filter(name="shop.OrderPlaced").count()
        print(f"   OrderPlaced events left:   {orders_left}   <- kept for RETENTION_DAYS")
        check("Retention.SUCCEEDED deletes a consumed event at once", reservations.count(), 0)
        check("and nothing kept by RETENTION_DAYS goes with it", orders_left, orders_logged)
        quiet = {entry.key for entry in quiet_receivers()}
        print(f"   shop.notify_warehouse quiet: {'shop.notify_warehouse' in quiet}")
        check(
            "the prune remembers that the receiver ran",
            "shop.notify_warehouse" in quiet,
            False,
        )

        head("14. The receipt mailer has a lane of its own, and a curve that lasts")
        receipt = "shop.email_receipt"
        replay_events([placed.pk], receiver_keys=[receipt])
        sent_before = SentEmail.objects.count()
        print(f"   default lane: {deliver_pending(worker_id='demo', lane='default')}")
        check(
            "a relay without --lane leaves the mail lane alone",
            SentEmail.objects.count(),
            sent_before,
        )
        print(f"   mail lane:    {deliver_pending(worker_id='demo', lane='mail')}")
        check("the mail lane's relay sends it", SentEmail.objects.count(), sent_before + 1)
        [mailer] = [r for e in catalogue().events for r in e.receivers if r.key == receipt]
        # One wait between each pair of attempts, each drawn from zero up to
        # its ceiling: the sum of the ceilings is the longest the budget can
        # last, and half of it is how long it lasts on average.
        ceilings = [
            backoff(
                n, base=mailer.backoff_base_seconds, cap=mailer.backoff_cap_seconds, jitter=1.0
            ).total_seconds()
            for n in range(1, mailer.max_attempts)
        ]
        print(f"   {mailer.max_attempts} attempts, waits of at most {[int(c) for c in ceilings]}s")
        print(f"   lasts at most {sum(ceilings) / 3600:g}h, {sum(ceilings) / 7200:g}h on average")
        check("the retry budget lasts at most three hours", sum(ceilings), 3 * 3600)
        check("and an hour and a half on average", sum(ceilings) / 2, 1.5 * 3600)

        head("15. One relay serves both lanes, each in turn")
        # The same two lanes, served by one process rather than one each: the
        # relay visits them in sorted order, one claim per lane under that
        # lane's own lease and batch size. A burst in one lane then delays the
        # other by one of its batches, never by the whole burst. --once takes
        # the same lanes, so this pass runs on SQLite too.
        audit = "shop.audit"
        replay_events([placed.pk], receiver_keys=[receipt, audit])
        sent_before = SentEmail.objects.count()
        before = {lane.lane: lane.owed for lane in outbox_health().lanes}
        print(f"   owed by lane: {before}")
        check(
            "outbox_health() lists every lane, the default one included",
            sorted(before),
            ["default", "mail"],
        )
        delivered = StringIO()
        call_command(
            "deliver_events",
            "--once",
            "--lane",
            "default",
            "--lane",
            "mail",
            "--lane-batch-size",
            "mail=5",
            "--worker-id",
            "demo",
            stdout=delivered,
        )
        print(
            f"   deliver_events --once --lane default --lane mail: {delivered.getvalue().strip()}"
        )
        replayed = dict(
            DeliveryRecord.objects.filter(
                event_id=placed.pk, receiver_key__in=[receipt, audit]
            ).values_list("receiver_key", "status")
        )
        check(
            "one pass delivers the replayed row in each lane",
            replayed,
            {receipt: "succeeded", audit: "succeeded"},
        )
        check("and sends the receipt once", SentEmail.objects.count(), sent_before + 1)
        status = StringIO()
        call_command("events_status", "--format", "json", stdout=status)
        after = {lane["lane"]: lane["owed"] for lane in json.loads(status.getvalue())["lanes"]}
        print(f"   events_status lanes: {after}")
        check("events_status shows each lane", sorted(after), ["default", "mail"])
        # The mail lane only: what else the default lane's turn finds due
        # depends on how the earlier steps' backoffs were drawn.
        check("and the mail lane is one row lighter", after["mail"], before["mail"] - 1)

        head("16. A reminder due a fortnight after the invoice")
        chase = "shop.chase_unpaid_invoice"
        # The billing system sent both invoices thirteen days ago, so the
        # reminders, due fourteen days after that, are due tomorrow. The
        # first order was cancelled in step 5; the second is still open.
        issued = now() - timedelta(days=13)
        with transaction.atomic():
            from django_domain_events import fire

            fire(InvoiceSent(order_id=order.pk), occurred_at=issued)
            fire(InvoiceSent(order_id=second.pk), occurred_at=issued)
        due = issued + timedelta(days=14)

        def reminders() -> list[DeliveryRecord]:
            # Read afresh each time: a queryset caches the rows it first
            # fetched, and every check below is about a later state.
            return list(DeliveryRecord.objects.filter(receiver_key=chase).order_by("pk"))

        print(f"   due at: {sorted({r.due_at.isoformat() for r in reminders()})}")
        check(
            "each reminder is due fourteen days after its invoice",
            {r.due_at for r in reminders()},
            {due},
        )
        # A drain skips the backoff - everything the earlier steps left
        # failing runs to its end here - and never skips a due time.
        drain_outbox()
        print(f"   after a drain: {[(r.status, r.attempts) for r in reminders()]}")
        check(
            "a drain leaves a reminder that is not due alone",
            [(r.status, r.attempts) for r in reminders()],
            [("pending", 0), ("pending", 0)],
        )
        health = outbox_health()
        [backlog] = [entry for entry in health.receivers if entry.key == chase]
        print(
            f"   scheduled={health.scheduled}; {chase}: owed={backlog.owed} scheduled={backlog.scheduled}"
        )
        check("health counts them as scheduled", (health.scheduled, backlog.scheduled), (2, 2))
        check("and not as owed, so no backlog alert fires", backlog.owed, 0)
        emails = SentEmail.objects.count()
        last_mail = SentEmail.objects.filter(to=user.email).latest("sent_at").sent_at
        quiet_until = last_mail + REMINDER_QUIET_PERIOD
        drain_outbox(now=due + timedelta(seconds=1))
        print("   drained as of tomorrow, when both reminders are due:")
        cancelled, open_order = reminders()
        print(
            f"   cancelled order: {cancelled.status}, emails sent: {SentEmail.objects.count() - emails}"
        )
        print(
            f"   open order:      {open_order.status}, due again at {open_order.due_at.isoformat()}"
        )
        check("moot work is an ordinary success", cancelled.status, "succeeded")
        check("and sends nothing", SentEmail.objects.count(), emails)
        check(
            "NotYet moves the open order's reminder past the quiet period, spending no attempt",
            (open_order.status, open_order.attempts, open_order.due_at),
            ("pending", 0, quiet_until),
        )
        check("and it is scheduled again", outbox_health().scheduled, 1)
        drain_outbox(now=quiet_until + timedelta(seconds=1))
        open_order.refresh_from_db()
        print(f"   after the quiet period: {open_order.status} after {open_order.attempts} attempt")
        check(
            "the reminder goes once it is due",
            (open_order.status, open_order.attempts),
            ("succeeded", 1),
        )
        check(
            "and is sent",
            SentEmail.objects.filter(subject__startswith="Reminder:").count(),
            1,
        )
        # The late case: the reminder is due, and the quiet period it was waiting
        # out ended before the row ran at all. Comparing against `ctx.due_at`
        # alone would raise `NotYet` with a time already come, which is refused
        # and recorded as a failure on every retry; flooring the comparison at
        # the clock sends the reminder.
        with transaction.atomic():
            from django_domain_events import fire

            late_invoice = fire(InvoiceSent(order_id=second.pk))
        [late] = DeliveryRecord.objects.filter(event=late_invoice, receiver_key=chase)
        # As if the relay had been down: the row came due six days ago and has
        # not been claimed since. (`fire` never writes a due time in the past,
        # so a row can only be this late by waiting.) Its customer was last
        # emailed four days ago, so the quiet period ended a day ago - after the
        # row came due and before it ran.
        six_days_ago = now() - timedelta(days=6)
        DeliveryRecord.objects.filter(pk=late.pk).update(
            available_at=six_days_ago, due_at=six_days_ago
        )
        SentEmail.objects.filter(to=user.email).update(sent_at=now() - timedelta(days=4))
        emails = SentEmail.objects.count()
        drain_outbox()
        late.refresh_from_db()
        print(
            f"   late reminder: {late.status} after {late.attempts} attempt, "
            f"error: {late.last_error or 'none'}"
        )
        check(
            "a reminder that runs after its quiet period ended is sent, not failed",
            (late.status, late.attempts, late.last_error),
            ("succeeded", 1, ""),
        )
        check("and sends exactly one email", SentEmail.objects.count(), emails + 1)
        head("17. The invoice is paid, so its reminder is cancelled")
        # A new invoice for the open order, and its reminder due in a
        # fortnight. Then the customer pays: the billing code knows the
        # reminder is moot ahead of time, so rather than leave it to run and
        # find nothing to do, it withdraws it, in the transaction that records
        # the payment.
        with transaction.atomic():
            from django_domain_events import fire

            invoice = fire(InvoiceSent(order_id=second.pk))
        [reminder] = DeliveryRecord.objects.filter(event=invoice, receiver_key=chase)
        emails = SentEmail.objects.count()
        with transaction.atomic():
            counts = cancel_deliveries([invoice], receiver_key=chase)
        print(f"   cancel_deliveries: {counts}")
        check(
            "the waiting reminder is cancelled, and none was in flight",
            counts,
            {"cancelled": 1, "in_flight": 0},
        )
        drain_outbox(now=reminder.due_at + timedelta(days=1))
        reminder.refresh_from_db()
        print(f"   drained past its due time: {reminder.status} after {reminder.attempts} attempts")
        check(
            "a drain past its due time never runs it",
            (reminder.status, reminder.attempts, reminder.succeeded_at),
            ("cancelled", 0, None),
        )
        check("and nothing is sent", SentEmail.objects.count(), emails)
        health = outbox_health()
        check(
            "health counts it as neither scheduled nor owed",
            (health.scheduled, [b.key for b in health.receivers if b.key == chase]),
            (0, []),
        )

        head("18. A receiver declared async def runs to completion")
        with transaction.atomic():
            from django_domain_events import fire

            delivered = fire(OrderDelivered(order_id=second.pk))
        print(f"   {deliver_pending(worker_id='demo')}")
        [review] = DeliveryRecord.objects.filter(
            event=delivered, receiver_key="shop.ask_for_a_review"
        )
        asked = SentEmail.objects.filter(subject=f"How was order {second.pk}?").count()
        print(f"   {review.receiver_key}: {review.status} after {review.attempts} attempt")
        print(f"   review requests sent: {asked}   <- the coroutine's own write")
        check(
            "an async def receiver's delivery succeeds",
            (review.status, review.attempts),
            ("succeeded", 1),
        )
        check("and its body ran, sending the request once", asked, 1)

        head("19. A hook around each delivery puts its ids on every line")
        # Restocked, so the INLINE check lets the next two orders through.
        StockLevel.objects.filter(sku="WIDGET").update(available=10)
        lines = _Lines()
        shop_log = logging.getLogger("shop")
        shop_log.addHandler(lines)
        try:
            third = place_order(user, "WIDGET", 1, 999)
            print(f"   {deliver_pending(worker_id='demo')}")
        finally:
            shop_log.removeHandler(lines)
        reserved = DeliveryRecord.objects.get(
            receiver_key="shop.reserve_stock", event__dedupe_key=f"order-placed:{third.pk}"
        )
        stamped = [delivery for delivery, line in lines.lines if line.endswith(f"order {third.pk}")]
        print(f"   reserve_stock logged: {stamped}   <- set by the hook, not the receiver")
        check(
            "the receiver's own line carries its delivery and receiver ids",
            stamped,
            [f"delivery={reserved.pk} receiver=shop.reserve_stock"],
        )
        check("and the hook put back what was there when it exited", current_delivery.get(), "-")

        head("20. A hook that cannot be entered fails the attempt")
        broken = {
            **django_settings.DJANGO_DOMAIN_EVENTS,
            "AROUND_DELIVERY": "shop.delivery_log.tracer_that_is_down",
        }
        with override_settings(DJANGO_DOMAIN_EVENTS=broken):
            fourth = place_order(user, "WIDGET", 1, 999)
            # The claims are made at a clock fixed before the failure, so the
            # backoff the failed row is given (a random fraction of two seconds,
            # which can be a few milliseconds) is always still running: without
            # it the pass sometimes retries the row it has just failed.
            print(f"   {deliver_pending(worker_id='demo', now=now())}")
        unreserved = DeliveryRecord.objects.get(
            receiver_key="shop.reserve_stock", event__dedupe_key=f"order-placed:{fourth.pk}"
        )
        print(
            f"   {unreserved.receiver_key}: {unreserved.status} after {unreserved.attempts} attempt"
        )
        print(f"         {unreserved.last_error}")
        check(
            "entering a hook that raises fails the attempt, which counts",
            (unreserved.status, unreserved.attempts),
            ("failed", 1),
        )
        check(
            "the row records the hook's error",
            unreserved.last_error,
            "ConnectionError: the tracing collector is unreachable",
        )
        check(
            "and the receiver never ran",
            Reservation.objects.filter(order=fourth).exists(),
            False,
        )

        head("21. A string longer than its column is refused before it is written")
        # SQLite stores a string of any length, so without these refusals each
        # of the four below would work here and fail on Postgres. Two are
        # refused when a declaration runs, which is at import in a real
        # project, so a too-long one stops it starting; the demo applies the
        # decorators itself to show that nothing is registered.
        too_long = "x" * 256
        events_before = EventRecord.objects.count()
        refusals: list[str] = []
        try:
            with transaction.atomic():
                from django_domain_events import fire

                fire(
                    OrderPlaced(
                        order_id=fourth.pk,
                        sku="WIDGET",
                        quantity=1,
                        total_cents=999,
                        currency="EUR",
                    ),
                    dedupe_key=too_long,
                )
        except ValueError as error:
            refusals.append("fire(dedupe_key=)")
            print(f"   {str(error).split(';')[0]}")
        try:
            with suppressed(OrderPlaced, reason=too_long):
                ...
        except ValueError as error:
            refusals.append("suppressed(reason=)")
            print(f"   {str(error).split(';')[0]}")
        long_key = "shop." + too_long
        try:

            @receiver(OrderPlaced, mode=DURABLE, key=long_key)
            def never_registered(order: OrderPlaced) -> None: ...

        except ValueError as error:
            refusals.append("@receiver(key=)")
            print(f"   {str(error).split(';')[0]}")
        long_name = "shop." + too_long
        try:

            @event(name=long_name)
            @dataclass(frozen=True)
            class NeverDeclared:
                value: int

        except ValueError as error:
            refusals.append("@event(name=)")
            print(f"   {str(error).split(';')[0]}")
        check(
            "each of the four is refused with ValueError",
            refusals,
            ["fire(dedupe_key=)", "suppressed(reason=)", "@receiver(key=)", "@event(name=)"],
        )
        check("the refused fire wrote no event row", EventRecord.objects.count(), events_before)
        check(
            "and neither declaration was registered",
            (registry.receiver_for_key(long_key), registry.event_for_name(long_name)),
            (None, None),
        )

        head("22. A delivery started inside its claim's lease writes no second one")
        # Every claim stamps a tenth more than the lease its lane's receivers
        # share, so a row started soon after it is already leased for as long
        # as its receiver needs, and the extension write before the receiver
        # runs is skipped. This lane's receivers do not share one - the audit
        # receiver declares lease_seconds=900 - so the claim stamps
        # LEASE_SECONDS (30) plus a tenth: reserve_stock starts inside it, and
        # the audit row is extended to its own 900 seconds when it starts.
        # Seen in the statements, because nothing else can see a write that
        # did not happen.
        fifth = place_order(user, "WIDGET", 1, 999)
        rows = {
            row.receiver_key: row.pk
            for row in DeliveryRecord.objects.filter(
                event__dedupe_key=f"order-placed:{fifth.pk}", status="pending"
            )
        }
        table = connection.ops.quote_name(DeliveryRecord._meta.db_table)
        column = connection.ops.quote_name("lease_expires_at")
        # The values each lease write was bound with: the new lease, then the
        # row's own fence, whose one integer is the row's primary key.
        bound: list[object] = []

        def watch(execute: Any, sql: str, params: Any, many: bool, context: Any) -> Any:
            if sql.startswith(f"UPDATE {table} SET {column} = "):
                bound.extend(params[1:])
            return execute(sql, params, many, context)

        with connection.execute_wrapper(watch):
            print(f"   {deliver_pending(worker_id='demo', lane='default')}")
        extended = sorted(key for key, pk in rows.items() if pk in bound)
        print(f"   default-lane rows owed: {sorted(rows)}")
        print(f"   extended before running: {extended}   <- leased longer than the claim")
        check(
            "only the receiver leased longer than the claim is extended",
            extended,
            ["shop.audit"],
        )
        print("\nEvery claim above was checked.")
