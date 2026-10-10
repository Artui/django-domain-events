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
| `outbox_health()` | whether the queue is draining, as opposed to whether a receiver is running |
| A backfill | `suppressed()` recording without delivering, and saying why |
| A row from before a field existed | the `upgrade()` hook migrating a v1 payload |
| The marketplace answers `410 Gone` | `PermanentFailure` dead-lettering on the attempt that raised it, one of five spent |
| The carrier asks for two minutes | `RetryAfter` scheduling the next attempt when it asked, and counting the attempt |
| The customs broker asks for two days | the request clamped to `MAX_RECEIVER_RETRY_DELAY_SECONDS`, with a warning naming both numbers |
| Two partners subscribe to orders | an `AnyEvent` receiver with `targets=`: one delivery row per partner, and none for an event nobody subscribed to |
| The partners change, then a replay | `replay_events` asking for the targets again: a partner still subscribed is reopened, a new one added, one that left untouched |
| A consumed event, then a prune | `Retention.SUCCEEDED` deleting `StockReserved` once delivered while the orders stay for `RETENTION_DAYS`, and `quiet_receivers` still knowing its receiver ran |

| The receipt is replayed into its lane | `lane="mail"`: the default lane's pass leaves it alone and the mail lane's sends it; and the receipt's own retry curve, checked to last at most three hours and an hour and a half on average |

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
11. **`forward_to_partners`** - declared for `AnyEvent` with
    `targets=partners_subscribed`, because it is a transport: it forwards every
    event, to whichever partners a table says want it, with a delivery row per
    partner. Nothing subscribed means no row at all.
12. **`StockReserved`** - `retention=Retention.SUCCEEDED`, because it is
    bookkeeping between two of our own steps and worth nothing once delivered.
    The next prune deletes it with its delivery rows; a dead letter would keep it
    for `RETENTION_DAYS` so it could still be requeued.

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

Not shown here, and worth reading about instead:
`propagate_scope`, `drain_outbox`, the `task` execution site
and the `dacite` codec.
