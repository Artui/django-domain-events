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
   is real here. `eager=True` for latency, `max_attempts=8` because a mail
   provider being down for an hour is ordinary.
5. **`write_audit_trail`** - `takes_context=True` for the attribution the row
   carries, and `lease_seconds=900` because it is slow. A receiver cannot extend
   its own lease: it runs inside the transaction carrying its acknowledgement,
   so nothing it writes is visible to another worker until it has finished.
6. **`warm_cache`** - `ON_COMMIT`. Best effort, no row, no retry, which is right
   for work that is pure optimisation.
7. **`refund`** - deliberately broken, so the dead-letter path is visible.

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
DDE_EXAMPLE_DATABASE=postgres python manage.py deliver_events
```

Not shown here, and worth reading about instead: `replay_events`,
`prune_events`, `propagate_scope`, `drain_outbox`, the `task` execution site
and the `dacite` codec.
