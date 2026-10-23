# The example shop

A small Django project that uses the declaration and delivery surface of
`django-domain-events` for something a real application would actually want. It is here to be read as much
as run: each receiver in [`shop/events.py`](shop/events.py) exists to show one
knob doing one job, with the reason in its docstring.

## Run it

```bash
cd examples/shop
python manage.py migrate
python manage.py demo
```

SQLite by default, so there is nothing to set up. The demo prints what the log
actually recorded at each step - and **checks** each of those claims, so it
exits non-zero if any of them stops being true. That is what makes it safe to
run in CI as documentation that cannot drift.

For the half SQLite cannot show - more than one relay over one queue, which
needs `SELECT ... FOR UPDATE SKIP LOCKED`:

```bash
createdb dde_example_shop
DDE_EXAMPLE_DATABASE=postgres python manage.py migrate
DDE_EXAMPLE_DATABASE=postgres python manage.py demo
```

## What it demonstrates

| In the demo | Shows |
| --- | --- |
| An oversized order is refused | `INLINE` vetoing the sale by raising, and the event rolling back with it |
| A sale goes through | the event row and the delivery rows written in the caller's transaction |
| The receipt is already sent | `eager=True` - outbox durability at on-commit latency |
| The relay delivers the rest | `DURABLE`, effectively once for the receivers that touch only this database |
| An event fired inside a receiver | causation recorded with nothing threaded through the call site |
| The refund keeps failing | backoff, dead-lettering, and `requeue_dead` |
| `outbox_health()` | whether the queue is draining, as opposed to whether a receiver is running, and per receiver how many of its rows a worker holds, checked against a row a relay claimed and stopped on (`claim_batch`), which `lapsed_leases` counts too |
| A backfill | `suppressed()` recording without delivering, and saying why |
| A row from before a field existed | the `upgrade()` hook migrating a v1 payload |
| The marketplace answers `410 Gone` | `PermanentFailure` dead-lettering on the attempt that raised it, one of five spent |
| The carrier asks for two minutes | `RetryAfter` scheduling the next attempt when it asked, and counting the attempt |
| The customs broker asks for two days | the request clamped to `MAX_RECEIVER_RETRY_DELAY_SECONDS`, with a warning naming both numbers |
| The mail quota is spent for the day | `RetryAfter(counts=False)` spending no attempt and coming back between the hour asked and twice that, bounded by `give_up_after` |
| Two partners subscribe to orders | an `AnyEvent` receiver with `targets=`: one delivery row per partner, and none for an event nobody subscribed to |
| The partners change, then a replay | `replay_events` asking for the targets again: a partner still subscribed is reopened, a new one added, one that left untouched |
| A consumed event, then a prune | `Retention.SUCCEEDED` deleting `StockReserved` once delivered while the orders stay for `RETENTION_DAYS`, and `quiet_receivers` still knowing its receiver ran |
| The receipt is replayed into its lane | `lane="mail"`: the default lane's pass leaves it alone and the mail lane's sends it; and the receipt's own retry curve, checked to last at most three hours and an hour and a half on average |
| One relay serves both lanes | `deliver_events --once --lane default --lane mail --lane-batch-size mail=5`: one pass visits each lane in turn, under its own lease and batch size, delivers the replayed receipt and audit row in a single run, and `outbox_health().lanes` and `events_status` list both lanes, the mail lane one row lighter after it |
| Invoice reminders due a fortnight later | `due=timedelta(days=14)` counted from the invoice's `occurred_at`: `drain_outbox()` leaves the reminders alone, `outbox_health()` counts them as `scheduled` rather than owed, and `drain_outbox(now=...)` runs them - the cancelled order's as moot work that succeeds and sends nothing, the open order's answering `NotYet` until three days after the customer's last email, then sending; one that runs after that quiet period has already ended is sent rather than failed on a past `NotYet` |
| The invoice is paid before the reminder | `cancel_deliveries` withdrawing the reminder in the transaction that records the payment: it settles as `cancelled`, a drain past its due time never runs it, nothing is sent, and `outbox_health()` counts it nowhere |
| The customer is asked for a review | an `async def` receiver run to completion by the relay: its delivery succeeds and the request its coroutine wrote through the async ORM is on record, once |
| Every delivery's lines name it | `AROUND_DELIVERY`: a hook entered around each durable attempt sets a log context, so the receiver's own line carries its delivery and receiver ids, as does the relay's warning for the marketplace's dead letter, and the context is gone once the attempt ends |
| The tracer is down | a hook that raises on entering fails the attempt as the receiver raising would: the row is `failed` after one attempt with the hook's error, and the receiver never ran |
| Strings too long for their columns | a 256-character `dedupe_key` refused by `fire()` with no event row written, the same reason refused by `suppressed()` on entry, and a receiver `key=` and an event `name=` over 255 characters refused by their decorators with nothing registered; SQLite would have stored all four, so these are what keep them from failing only on Postgres |
| A delivery started inside its claim's lease | a claim stamping the lease a lane's receivers share plus a tenth, so `reserve_stock` starts with no second lease write, while `shop.audit`, leased for 900 seconds against the claim's 33, is extended to its own lease when it starts; checked by watching the statements, since a write that did not happen leaves nothing else to see |

## The declarations

`shop/events.py` is the whole surface. Worth reading in this order:

1. **`OrderPlaced`** - versioned, with an `upgrade()` hook, because v2 added a
   required field and every row written before that deploy would otherwise
   dead-letter.
2. **`refuse_orders_we_cannot_fill`** - `INLINE`. The only mode allowed to abort
   the caller's work, and the reason it needs no durability: its failure is a
   rollback, so nothing can be owed.
3. **`reserve_stock`** - `DURABLE`, touching only this database, so its write and
   its acknowledgement commit together and the duplicate at-least-once entitles
   you to cannot be observed. It also fires a second event.
4. **`email_receipt`** - a side effect the database cannot undo, so at-least-once
   is real here. `eager=True` for latency. A retry curve of its own -
   `backoff_base_seconds=120`, `backoff_cap_seconds=1800`, `max_attempts=10` -
   because a mail provider being down for an hour is ordinary: nine waits whose
   ceilings sum to three hours, so an hour and a half on average, since full
   jitter draws each wait from zero up to its ceiling. The settings' 2-second
   base would have spent eight attempts in at most 254 seconds. And
   `lane="mail"`, so sending runs on relays of its own and a slow provider holds
   up nothing else.
5. **`write_audit_trail`** - `takes_context=True` for the attribution the row
   carries, and `lease_seconds=900` because it is slow. A receiver cannot extend
   its own lease: it runs inside the transaction carrying its acknowledgement,
   so nothing it writes is visible to another worker until it has finished.
6. **`warm_cache`** - `ON_COMMIT`. Best effort, no row, no retry, which is right
   for work that is pure optimisation.
7. **`refund`** - deliberately broken, so the dead-letter path is visible.
8. **`notify_marketplace`** - `PermanentFailure`, because the marketplace has
   said `410 Gone` and four more attempts across the next hour would only be
   four more refusals.
9. **`register_tracking`** - `RetryAfter`, because the carrier's rate limiter
   said when to come back, and the backoff curve would only guess. It still
   costs an attempt, so a limiter that never relents still dead-letters.
10. **`book_customs_clearance`** - `RetryAfter` past the ceiling. A delivery
    parked in the future is still owed and keeps its event past retention, so
    the relay caps the wait and says so.
11. **`email_dispatch_notice`** - `RetryAfter(counts=False)`, because a spent
    daily quota is a limit on the mail provider, not a failure of the email. It
    spends no attempt, comes back between the hour asked and twice that, and
    pauses the mail lane in the relay that heard it; `give_up_after=timedelta(days=2)`
    is what still ends it.
12. **`forward_to_partners`** - declared for `AnyEvent` with
    `targets=partners_subscribed`, because it is a transport: it forwards every
    event, to whichever partners a table says want it, with a delivery row per
    partner. Nothing subscribed means no row at all.
13. **`StockReserved`** - `retention=Retention.SUCCEEDED`, because it is
    bookkeeping between two of our own steps and worth nothing once delivered.
    The next prune deletes it with its delivery rows; a dead letter would keep it
    for `RETENTION_DAYS` so it could still be requeued.
14. **`chase_unpaid_invoice`** - `due=timedelta(days=14)`, because a reminder
    two weeks after the invoice is a reaction to the invoice that runs later,
    and a beat job polling every minute for invoices to chase finds nothing
    almost every time. A cancelled order makes the reminder moot, and returning
    is an ordinary success. A customer emailed in the last three days makes it
    not due yet, and returning would lose it, so it raises `NotYet` with the end
    of the quiet period - comparing against `ctx.due_at` floored at the clock
    (`max(ctx.due_at, timezone.now())`), so a reminder that runs after the quiet
    period has ended is sent rather than failing on a past `NotYet`.
15. **`ask_for_a_review`** - `async def`, because the review platform's client
    is async. It runs to completion on an event loop of its own wherever its
    delivery runs, and its async ORM write commits with the acknowledgement,
    as a sync receiver's does. A client is made inside the call, never at
    module level, because each call's loop is closed when it returns.

Beside them, **`shop/delivery_log.py`** holds the one hook the settings name,
`AROUND_DELIVERY`. It is entered around every durable attempt, outside the
transaction that carries the acknowledgement, and sets the delivery and
receiver ids for the length of it; the `LOGGING` filter in `settings.py` puts
them on every line, which is why the demo's warnings read
`WARNING [delivery=<id> receiver=shop.refund]`. No receiver sets them itself.
`tracer_that_is_down` beside it is the failing state, which the demo switches
to for one order: a hook that cannot be entered fails the attempt rather than
letting the receiver run without it.

## Other things to try

```bash
python manage.py export_catalogue          # every event, its payload, its receivers
python manage.py events_status             # how far behind the outbox is
python manage.py quiet_receivers --days 1  # what has not run
python manage.py createsuperuser           # then browse /admin/ for the log
```

The long-running relay needs Postgres, because claiming rows without handing
the same one to two workers needs `SELECT ... FOR UPDATE SKIP LOCKED`. On
SQLite it refuses to start rather than pretend:

```bash
DDE_EXAMPLE_DATABASE=postgres python manage.py deliver_events              # the default lane
DDE_EXAMPLE_DATABASE=postgres python manage.py deliver_events --lane mail  # the receipts
```

Or one relay for both, taking the lanes in turn, with the receipts kept to
small batches so a burst of them delays the rest by one batch at most:

```bash
DDE_EXAMPLE_DATABASE=postgres python manage.py deliver_events --lane default --lane mail --lane-batch-size mail=5
```

Not shown here, and worth reading about instead:
`propagate_scope`, the `task` execution site and the `dacite` codec.
