# Delivery

## Two knobs, not one enum

Timing and execution site are separate questions, and conflating them is how
"put it on a queue" ends up meaning "and also change when it runs".

### Knob 1 - timing and guarantee

Declared per receiver as `mode=`.

| Mode | Runs | Can veto the change | Recoverable |
| --- | --- | --- | --- |
| `INLINE` | inside the firing transaction | **yes**, by raising | not needed: its failure is a rollback |
| `ON_COMMIT` | after commit, in the firing process | no | no |
| `DURABLE` (default) | after commit, at-least-once, retried | no | **yes** |

`INLINE` needs no durability precisely because its failure mode is a rollback:
if it raises, the change never committed, so nothing can be owed. That is also
the only mode that may legitimately abort the caller's work.

`ON_COMMIT` is best effort and honest about it. A process that dies between
commit and callback loses the delivery, with no row to say so.

`DURABLE` writes a row per receiver. One failing receiver must not replay or
block the other four, which is why the debt is per-receiver rather than one
outbox row per event.

!!! tip "Effectively once, for database-only receivers"
    For a receiver that touches only this database, the work and the
    acknowledgement commit together. The duplicate an at-least-once system owes
    you cannot be observed. Receivers with side effects outside the database -
    an email, a webhook - are at-least-once, as promised, and need their own
    idempotency.

### Knob 2 - execution site

Meaningful only for `DURABLE`, because it is the only mode with a row to hand
somewhere else.

- `site="relay"` (default) - the relay process runs the receiver.
- `site="task"` - the relay enqueues the delivery id to the configured
  [task backend](operations.md#handing-delivery-to-a-task-queue) and moves on.

A queue is only ever an answer to *where*. It is not a timing mode, and adopting
one does not change what the event promised.

## Eager delivery

```python
@receiver(OrderPlaced, eager=True)
def notify(evt: OrderPlaced) -> None: ...
```

`eager=True` attempts the receiver immediately after commit, in the firing
process, **with the relay as the fallback**. The row still exists; a failed
eager attempt is simply owed to the relay like any other. It buys latency
without giving up the crash story.

## The relay

```bash
python manage.py deliver_events            # forever
python manage.py deliver_events --once     # one pass, for cron or CI
python manage.py deliver_events --limit 100 --worker-id box-1
```

The relay claims with `SELECT ... FOR UPDATE SKIP LOCKED` under a **lease**, so
you can run as many as you like:

- Two workers never take the same row.
- A worker that dies without acknowledging has its rows reclaimed when the lease
  lapses.
- The acknowledgement is a **compare-and-set** on `(claimed_by, claimed_at)`, so
  a worker whose lease already lapsed and was stolen cannot overwrite the new
  owner's result.

!!! warning "SKIP LOCKED is Postgres and MySQL 8"
    SQLite has neither the statement nor the concurrency model that would make
    it meaningful. Declaration, `INLINE`, `ON_COMMIT`, `fire()`, the tables and
    `drain_outbox()` all work on every backend, so **a SQLite test suite is
    fully supported** - but running more than one relay is not.

## Failure

A `DURABLE` receiver that raises is retried with **exponential backoff and full
jitter**, up to `max_attempts`, then dead-lettered:

```
ceiling      = min(BACKOFF_BASE_SECONDS * 2 ** (attempt - 1), BACKOFF_CAP_SECONDS)
available_at = now + ceiling * random()
```

Full jitter draws from the **whole** window up to the ceiling rather than
adding a little on top of it. Retrying a shared downstream at
ceiling-plus-a-bit keeps every failed delivery in the same cohort, which is the
thundering herd the backoff was meant to break up.

Dead is where a delivery stops **on its own**, not where it stops for good - see
[requeue](operations.md#requeue-from-the-dead-letter-queue).

| Status | Means |
| --- | --- |
| `pending` | owed, waiting for `available_at` |
| `claimed` | leased by a worker |
| `succeeded` | ran, acknowledged |
| `failed` | raised, attempts remaining |
| `dead` | out of attempts |
| `orphaned` | addressed to a receiver key the registry no longer has |

`failed` is distinct from `pending` so that "has this ever failed" is answerable
without reading the attempt count.

## In tests

```python
from django_domain_events import assert_fired, drain_outbox


def test_placing_an_order_reserves_stock():
    place_order()
    assert_fired(OrderPlaced, times=1)
    drain_outbox()
```

`drain_outbox()` runs the **real** delivery path to completion - the same claim,
encode, decode and acknowledgement the relay performs - so a test exercising it
exercises production. `assert_fired` reads the log rather than a mock, which
means it also proves the row was written inside the transaction that committed.
