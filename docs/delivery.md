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
outbox row per event. A receiver declared with
[`targets=`](declaring.md#fan-out-one-delivery-per-target) takes that one step
further and writes a row per target, for the same reason.

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
- `site="task"` - the relay enqueues the delivery id, with the claim it holds,
  to the configured [task backend](operations.md#handing-delivery-to-a-task-queue)
  and moves on. The task takes the row under that claim before running it, so a
  queue that delivers a message twice or late runs the receiver once.

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
- A relay told to stop (`SIGTERM` or `SIGINT`) finishes the delivery in hand
  and **hands back** the rest of its batch. Another relay can claim those rows at
  once instead of waiting for the lease to lapse. See
  [Stopping it](operations.md#stopping-it).
- A relay whose database goes away **stays up**: it closes the dead connection,
  backs off and claims again. See
  [When the database goes away](operations.md#when-the-database-goes-away).

A `DURABLE` receiver runs inside the transaction that carries its
acknowledgement, so a call it makes outside the database holds that
transaction open. See
[A receiver holds a transaction open](operations.md#a-receiver-holds-a-transaction-open)
for what that means behind pgbouncer and under
`idle_in_transaction_session_timeout`.

!!! warning "SKIP LOCKED is Postgres and MySQL 8"
    SQLite has neither the statement nor the concurrency model that would make
    it meaningful. Declaration, `INLINE`, `ON_COMMIT`, `fire()`, the tables and
    `drain_outbox()` all work on every backend, so **a SQLite test suite is
    fully supported** - but running more than one relay is not.

## Failure

A `DURABLE` receiver that raises is retried with **exponential backoff and full
jitter**, up to `max_attempts`, then dead-lettered - unless it says otherwise,
which is [when the receiver knows better](#when-the-receiver-knows-better):

```
ceiling      = min(BACKOFF_BASE_SECONDS * 2 ** (attempt - 1), BACKOFF_CAP_SECONDS)
available_at = now + ceiling * random()
```

Full jitter draws from the **whole** window up to the ceiling rather than
adding a little on top of it. Retrying a shared downstream at
ceiling-plus-a-bit keeps every failed delivery in the same cohort, which is the
thundering herd the backoff was meant to break up.

### A curve per receiver

The two settings are the curve for every receiver. One that talks to a
destination with its own idea of patience declares its own:

```python
@receiver(OrderPlaced, backoff_base_seconds=120, backoff_cap_seconds=1800, max_attempts=10)
def email_receipt(evt: OrderPlaced) -> None: ...
```

Either may be declared alone, and the other comes from its setting. Declaring
both with the cap below the base is refused at the decorator, because every
retry would wait for the cap and the base would never be read.

The curve is read when an attempt fails, not copied onto the row, so a deploy
that changes it applies to deliveries already in flight. `max_attempts` is the
opposite on purpose: it is copied at fire time, so lowering it cannot
dead-letter rows already owed.

**Full jitter applies to a declared curve too, and it is easy to misread.** A
base of 60 does not mean "retry in a minute"; it means "retry somewhere in the
next minute", and the first retry can land after a few seconds. The ceilings
above are the longest each wait can be, and on average a wait is half its
ceiling. So the span a retry budget covers is a distribution, not a number:

- the **sum of the ceilings** is the longest it can last;
- **half of that** is how long it lasts on average.

The curve above, with ten attempts, has nine waits with ceilings of 120, 240,
480, 960 and then 1800 seconds five times: at most three hours, an hour and a
half on average. It is the curve the [example shop](https://github.com/Artui/django-domain-events/tree/main/examples/shop)
gives its receipt mailer, and the demo there checks both numbers. Eight
attempts on the default two-second base wait at most 254 seconds in all, so a
mail provider down for an hour dead-letters every delivery sent into it.

A list of delays or a callable schedule is not offered, because the
[catalogue](introspection.md#the-catalogue) publishes the curve and could not
publish either.

Dead is where a delivery stops **on its own**, not where it stops for good - see
[requeue](operations.md#requeue-from-the-dead-letter-queue).

| Status | Means |
| --- | --- |
| `pending` | owed, waiting for `available_at` |
| `claimed` | leased by a worker |
| `succeeded` | ran, acknowledged |
| `failed` | raised with attempts remaining, or deferred |
| `dead` | out of attempts, or deferred past `give_up_after` |
| `orphaned` | addressed to a receiver key the registry no longer has |

`failed` is distinct from `pending` so that "has this ever failed" is answerable
without reading the attempt count.

## Recording a failed attempt

A receiver cannot keep a record of its own failure by writing one: it runs inside
the transaction that carries its acknowledgement, so everything it wrote is
rolled back the moment it raises. `on_failure` is called afterwards, outside that
transaction and after the delivery row is updated, so what it writes survives.

```python
from django_domain_events import DeliveryFailure, receiver


def log_failure(failure: DeliveryFailure) -> None:
    DeliveryLog.objects.create(
        delivery_id=failure.delivery_id,
        attempt=failure.attempt,
        status=failure.status,
        error=failure.error,
    )


@receiver(OrderPlaced, on_failure=log_failure)
def notify_partner(evt: OrderPlaced) -> None: ...
```

It is called for `failed` and for `dead`, because "it failed again" and "it will
not be tried again" are different things to record. `DeliveryFailure` carries the
delivery's identity - `delivery_id`, `event_id`, `event_name`, `receiver_key`,
and the `target` for a fan-out receiver - with the `attempt`, the `status` and
the stored `error`, rather than the row itself, since another worker may own the
row by the time the hook runs.

A worker whose lease lapsed does not call it: whoever holds the row now will
report its own outcome. A hook that raises is logged and swallowed, because a
failure path that fails leaves the operator a traceback about logging instead of
about the delivery.

## When the receiver knows better

The backoff curve is a guess, and the attempt budget is the only thing that ends
a delivery on its own. Two exceptions let a durable receiver replace either with
what it actually knows.

```python
from django_domain_events import PermanentFailure, RetryAfter, receiver


@receiver(OrderPlaced)
def notify_partner(evt: OrderPlaced) -> None:
    response = post_to_partner(evt)
    if response.status_code == 410:
        raise PermanentFailure("410 Gone: the partner retired this endpoint")
    if response.status_code == 429:
        seconds = float(response.headers["Retry-After"])
        raise RetryAfter(seconds=seconds, reason=f"rate limited for {seconds:g}s")
    response.raise_for_status()
```

`PermanentFailure` dead-letters the row **on the attempt that raised it**. A
destination that has said it will never accept another request is not asked four
more times across the next hour. The row is `dead`, and `on_failure` is called
with `DEAD`, exactly as it is when the budget runs out.

`RetryAfter` schedules the next attempt `seconds` from now **instead of drawing
from the curve**. A rate limiter that says "in two minutes" is answering the
question the curve is guessing at, and arriving early only earns another refusal.

!!! warning "By default, a requested retry consumes an attempt"
    Otherwise a destination answering `429` to every request would keep a row
    alive forever and nothing would ever declare it dead. Every delivery stays
    bounded by the budget it was fired with. A receiver expecting to be rate
    limited should declare a wider `max_attempts` - or
    [defer without counting](#a-deferral-that-spends-no-attempt), bounded by
    time instead.

The request is capped at `MAX_RECEIVER_RETRY_DELAY_SECONDS`, a day by default,
and a longer one is clamped with a warning naming both numbers. The cap is its
own setting rather than `BACKOFF_CAP_SECONDS`, which bounds a curve: clamping an
hour-long `Retry-After` to a backoff ceiling would hammer a destination that asked
to be left alone.

Both are read by the relay and by nothing else. An `INLINE` receiver raising
either fails the caller's transaction like any other exception, and an
`ON_COMMIT` one has it logged: neither has a row to schedule or dead-letter. The
type is the signal, never the message - a `RuntimeError` saying "410 Gone" is an
ordinary failure.

## A deferral that spends no attempt

A throttle or a quota is a limit on the destination, not a failure of the row.
Counting it against `max_attempts` dead-letters good deliveries for being sent
at a busy moment, and widening the budget to survive a burst also widens it for
the failures it exists to end. `RetryAfter(seconds, counts=False)` says so:

```python
from datetime import timedelta

from django_domain_events import RetryAfter, receiver


@receiver(ReceiptQueued, lane="mail", give_up_after=timedelta(days=2))
def send_receipt(evt: ReceiptQueued) -> None:
    try:
        provider.send(render_receipt(evt))
    except provider.Throttled as exc:
        raise RetryAfter(exc.retry_after, reason="sending rate exceeded", counts=False)
    except provider.DailyQuotaExceeded:
        raise RetryAfter(seconds_until_quota_resets(), reason="daily quota", counts=False)
```

Three things happen, and each answers a way a plain retry goes wrong in a burst
of twenty thousand rows:

- **The attempt is not counted.** `attempts` is unchanged and `max_attempts` is
  not spent, so the row is still owed its whole budget for real failures. It is
  recorded as `failed`, with the message as `last_error`, and `on_failure` is
  called with `FAILED` and the attempt number the receiver's context carried -
  which the next run is given again, because nothing was spent.
- **The next attempt is jittered upwards**: between `seconds` and twice that
  from now, clamped to `MAX_RECEIVER_RETRY_DELAY_SECONDS`. Never earlier than
  the destination asked, and spread, so rows deferred together do not all come
  back in the same second and get throttled together again.
- **The relay pauses the receiver's lane** for `seconds`, and hands back the
  rest of the batch it had claimed rather than attempting it. Without that,
  learning that a daily quota is gone would cost one call per row: twenty
  thousand calls to hear the same answer twenty thousand times. With it, the
  relay that heard it spends no more calls on the lane until the delay has
  passed; the rows it gave back keep their place at the head of the queue and
  are attempted when the pause ends, one at a time, so if the quota is still
  gone it costs one call per pause, not one per row.

### `give_up_after` keeps every delivery ending

A deferral that does not count cannot be ended by `max_attempts`, and a
destination that throttles forever would keep its rows owed forever - holding
their events past [retention](retention.md), since a prune waits for every
delivery of an event. So it is bounded by **time**: the receiver declares
`give_up_after`, and a deferral arriving once the row has been owed that long
dead-letters it, exactly as a spent budget does - `dead`, `completed_at`,
`on_failure` with `DEAD`, and a `last_error` naming the bound.

"Owed" is measured from when the row became owed: when it was written, or when
[replay](operations.md#replay) or [requeue](operations.md#requeue-from-the-dead-letter-queue)
last reopened it. Both reset that moment, so a month-old event replayed into a
throttled destination is not dead-lettered on its first deferral for being a
month old. Backoff never moves it, so deferring cannot keep resetting the clock.
At exactly `give_up_after` the bound is reached.

The bound applies to deferrals alone. A receiver declaring it still has
ordinary failures and counting `RetryAfter`s ended by `max_attempts`, however
long the row has been owed.

!!! warning "`counts=False` without `give_up_after` counts"
    A receiver raising `counts=False` that declares no `give_up_after` has
    nothing that would ever end its deliveries. The deferral is then treated
    as an ordinary `RetryAfter` - counted, no jitter, no pause - and a warning
    naming the receiver is logged once per process. Declare the bound to get
    the deferral.

### The pause is per process

Nothing is shared between relays: each process pauses a lane when a deferral it
ran says to, measured on its own clock, and every relay serving the lane learns
of the throttle from its own first deferral - one call each. That is the price
of having no shared state to keep consistent, and it is small next to one call
per row. A relay serving the lane claims nothing while the pause lasts, and
resumes at its first pass after it ends, so within `POLL_SECONDS`.

A relay serving every lane - `run_relay(lane=None)`, or `deliver_pending()` and
`drain_outbox()` from code - pauses only the deferring lane: its unstarted rows
in the batch go back, the other lanes' rows in the same batch are still
delivered, and later claims leave the paused lane out. `deliver_pending()`
has no clock to wait on, so it sets the lane aside for the rest of the call.
Neither the pause nor the hand-back reaches a row already given to a
[task backend](operations.md#handing-delivery-to-a-task-queue), and a receiver
running in a task defers there, in a process that holds no batch to pause; its
deferral is recorded all the same. A direct `deliver_one()` likewise records
and pauses nothing, and so does an `eager=True` receiver's attempt at commit:
the firing process serves no lane, so in a burst every fire calls the
throttled destination once before the relay's pause can apply.

!!! note "For an endpoint's `429`"
    [django-outbound-webhooks](https://github.com/Artui/django-outbound-webhooks)
    raises `RetryAfter` for an endpoint answering `429`. That is the case this
    exists for, and it can adopt `counts=False` with a `give_up_after` on its
    receiver, so a rate-limited endpoint stops costing deliveries their budget.
    A receiver declared with `site="task"` keeps the unspent attempt and
    loses the pause, for the reason above.

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
