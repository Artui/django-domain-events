# Settings

Everything lives under one dict, so a project's settings file gains one name
rather than a dozen:

```python
DJANGO_DOMAIN_EVENTS = {
    "RETENTION_DAYS": 30,
    "LEASE_SECONDS": 120,
}
```

Every key has a default; the dict is merged over them, so you set only what you
are changing.

| Key | Default | What it does |
| --- | --- | --- |
| `CODEC` | `...DataclassCodec` | Encodes and decodes payloads. See [codecs](declaring.md#codecs). |
| `WARN_OUTSIDE_ATOMIC` | `True` | Warn when `fire()` is called with no transaction open. |
| `BATCH_SIZE` | `50` | Rows per claim and per requeue chunk. A relay may override it for its claims. |
| `LEASE_SECONDS` | `300` | The lease a delivery needs where its receiver declares none; a claim stamps its lane's lease x 1.1 ([operations](operations.md#the-lease-a-claim-stamps)). |
| `POLL_SECONDS` | `1.0` | Relay poll interval, and the floor under `LISTEN`/`NOTIFY`. |
| `WAKE` | `"notify"` | `"notify"` or `"poll"`. `"poll"` sends no `NOTIFY` and the relay does not `LISTEN`; latency is then `POLL_SECONDS`. |
| `NOTIFY_COALESCE_SECONDS` | `0.5` | A process sends at most one `NOTIFY` per this long, per database. `0` sends every one. |
| `BACKOFF_BASE_SECONDS` | `2.0` | First retry ceiling; doubles per attempt. |
| `BACKOFF_CAP_SECONDS` | `3600.0` | Ceiling the doubling stops at. |
| `MAX_RECEIVER_RETRY_DELAY_SECONDS` | `86400.0` | Longest delay a `RetryAfter` may ask for. |
| `RETENTION_DAYS` | `90` | Prune window for an event that declares no [retention](retention.md) of its own, and the default quiet-receiver window. |
| `PRUNE_BATCH_ROWS` | `5000` | Rows per prune transaction, delivery rows and event rows alike. |
| `RELAY_PRUNE` | `True` | Whether an idle relay runs `prune_events`. Turn it off if you schedule the prune yourself. |
| `RELAY_PRUNE_SECONDS` | `60` | The most often a relay sweeps, in seconds above zero. |
| `TASK_BACKEND` | `None` | Dotted path, or a `{"BACKEND": ..., **options}` mapping. |
| `AROUND_DELIVERY` | `None` | Dotted path to a callable taking a `DeliveryContext` and returning a context manager, entered around each durable attempt. |

## The ones worth thinking about

### `WARN_OUTSIDE_ATOMIC`

`fire()` outside a transaction still records the event, but it no longer means
what the package promises: the row can exist without the change committing,
which is the dual-write gap this exists to close. The warning is on by default
because that mistake is invisible otherwise.

Turn it off only where you genuinely intend an unconditional record - a backfill
script, say.

### `LEASE_SECONDS`

Too short and a slow receiver has its row stolen while it is still working. Too
long and a crashed worker's rows sit undelivered until the lease lapses.

The lease is a **fence**, not a hope: the acknowledgement is a compare-and-set on
`(claimed_by, claimed_at)`, so a worker whose lease lapsed and was stolen cannot
overwrite the new owner's result. A short lease costs duplicate work, never a
lost or double-recorded acknowledgement.

!!! warning "A receiver cannot extend its own lease, and no API offers to"
    It runs *inside* the transaction that carries its acknowledgement, so
    anything it writes - a lease extension included - is invisible to every
    other worker until it has already finished. Measured on Postgres: while the
    receiver had pushed its lease an hour out, another connection still read
    the original expiry.

    The answer is to declare the lease where it can still be published:

    ```python
    @receiver(RebuildIndex, lease_seconds=1800)
    def rebuild(evt): ...
    ```

    `outbox_health().lapsed_leases` tells you when you have got it wrong.

### `MAX_RECEIVER_RETRY_DELAY_SECONDS`

The ceiling on a receiver's own schedule, when it raises `RetryAfter`. A request
past it is clamped, with a warning naming the receiver, the delivery and both
numbers, so an operator can tell whether the ceiling or the destination is the
one to question.

Separate from `BACKOFF_CAP_SECONDS` on purpose. That setting bounds the
exponential curve, and a destination's `Retry-After` is not a point on a curve:
sharing one number would clamp an hour-long request to five minutes and hammer
the thing that asked to be left alone.

It is a ceiling at all because a delivery waiting in the future is still owed.
`prune_events` removes only settled events, so a row parked for a month keeps its
event past `RETENTION_DAYS` for that month, and a destination answering with an
absurd number would keep it indefinitely.

### `WAKE`

`"notify"` (the default) sends a `NOTIFY` after the commit of a `fire()` that
wrote a delivery row, and the relay waits on it. It only has an effect on
Postgres; elsewhere both settings are inert and the relay polls. `"poll"` turns
it off on Postgres too, which trades latency of up to `POLL_SECONDS` for a commit
that does not take Postgres's notification lock. The poll is the floor in both
modes, so neither can lose a delivery.

Read [Turning NOTIFY off](operations.md#turning-notify-off) before choosing: the
cost is real on some workloads and disputed on others, and polling is cheap
enough that lowering `POLL_SECONDS` is the usual companion to it. A relay behind
pgbouncer in transaction-pooling mode never hears a notification whatever this
says; see [Behind pgbouncer](operations.md#behind-pgbouncer).

A value other than `"notify"` or `"poll"` fails the system check `E006`, rather
than quietly meaning "poll".

### `NOTIFY_COALESCE_SECONDS`

A process sends at most one `NOTIFY` per this interval, per database. Skipping
one is safe for the reason losing one is: a single wake makes the relay claim
everything due, and the poll picks up the rest. The price is that an event
committed just after a notification, but after the relay had claimed, waits for
the next poll. `0` disables coalescing. A negative, non-numeric or NaN value
fails the system check `E007`.

### `BATCH_SIZE`

Two jobs, deliberately one number: it is "how many rows this package touches in
one statement", and the reasons to raise or lower it point the same way for both.

The requeue chunks on it because SQLite refuses more than 32,766 parameters in
one statement, and a dead-letter table past that is an ordinary outcome of one
bad deploy.

A relay can size its own claims without moving the other two:
`deliver_events --batch-size 5`, or `run_relay(batch_size=5)`. That is the
knob for a [lane](operations.md#lanes-a-relay-per-kind-of-work) of slow
receivers, which wants a batch it can work through inside the lease its claim
stamps ([operations](operations.md#the-lease-a-claim-stamps)) while pruning
keeps its larger statements.

### `PRUNE_BATCH_ROWS`

How many rows one prune transaction deletes, counting each event's delivery rows
and the event row itself. A setting of its own, rather than `BATCH_SIZE`, because
the unit differs: a claim takes delivery rows one receiver at a time, while a
prune takes whole events, and one fan-out event can carry tens of thousands of
rows. Counted in events, a batch's size was whatever the events in it happened to
hold.

An event with more delivery rows than this has them deleted this many at a time,
each chunk in its own transaction, and goes with the last of them. Override it for
one run with `prune_events --batch-size`. A value that is not a positive whole
number fails the system check `E010`, rather than failing every sweep of an idle
relay quietly. See [pruning](operations.md#pruning).

### `RELAY_PRUNE`

Whether a relay that finds nothing to claim runs `prune_events()`, which is what
makes an event declared to be [deleted on consumption](retention.md) go within
about a minute without a cron line. On by default, because a retention policy
that nothing carries out is the quiet failure. Set it to `False` when `prune_events`
runs from a schedule of its own, or when you run the relay only as
`deliver_events --once`, which never sweeps either way.

Every long-running relay sweeps, whatever its `--lane`; the throttle is what
keeps several cheap. A value that is not a bool fails the system check `E008`:
`"false"` is a truthy string and would leave the sweep on. See
[how the prune runs](retention.md#how-the-prune-runs).

### `RELAY_PRUNE_SECONDS`

The most often a relay sweeps, counted from the previous sweep on a monotonic
clock; the first comes this long after the relay starts. A sweep that fails also
waits a full interval. Raise it before turning the sweep off if the cost of
an idle sweep, which grows with the live events that carry a retention of their
own, matters on your database. Zero, a negative, NaN, infinity or a non-number
fails the system check `E009`; `RELAY_PRUNE = False` is the way to say never.

### `TASK_BACKEND`

A mapping rather than only a dotted path, because a backend with any constructor
options at all would otherwise be unreachable through the documented setting and
only a subclass could use it.

```python
"TASK_BACKEND": {
    "BACKEND": "django_domain_events.delivery.django_tasks_backend.DjangoTasksBackend",
    "queue_name": "events",
}
```

or, with the `celery` extra installed:

```python
"TASK_BACKEND": {
    "BACKEND": "django_domain_events.delivery.celery_backend.CeleryBackend",
    "queue": "events",
}
```

The Celery worker also needs the task module in its `imports`; see
[Celery](operations.md#celery).

### `AROUND_DELIVERY`

A dotted path to a callable that takes the attempt's `DeliveryContext` and
returns a synchronous context manager. The package enters it around every
durable attempt, outside the transaction that carries the acknowledgement, and
exits it once the outcome is recorded: the place for a tracing span, a log
context carrying `delivery_id` and `receiver_key`, or a tenant schema. One
callable, not a list; compose several with `contextlib.ExitStack`.

```python
"AROUND_DELIVERY": "myproject.delivery_log.delivery_log_context",
```

Read on every delivery, not cached. A path that does not import, a value that
is not a string, or a name that is not callable fails the system check `E011`
rather than the first delivery. Entering the hook raising fails the attempt as
the receiver raising would; an `Exception` from `__exit__` is logged and changes
nothing.

The hook must not open a transaction. The outcome is written after the
receiver's transaction closes and before `__exit__` runs, so a
`transaction.atomic()` the hook entered would hold that write, and exiting it
with the receiver's exception would roll the recorded failure back after the
log line and `on_failure` reported it. Set a tenant schema or role at session
level (`SET search_path`, `SET ROLE`) and reset it on exit, rather than with
`SET LOCAL`.

The rules, and what it never wraps, are in
[A hook around each delivery](delivery.md#a-hook-around-each-delivery).

## `USE_TZ = False`

Supported. Every datetime the package writes or compares follows the project's
setting: "now" is `django.utils.timezone.now()`, so it is aware under
`USE_TZ = True` and naive local time - in `TIME_ZONE` - under `False`, which is
how Django stores and returns the columns it is compared with.

A datetime your code hands in is converted to that form first: a `now=`
argument to `drain_outbox`, `deliver_pending`, `outbox_health`,
`quiet_receivers`, `prune_events`, `claim_batch` or `run_relay`'s clock, a
[`NotYet`](scheduling.md#when-the-work-is-not-due-yet-notyet) time, what a
[`due=`](scheduling.md) callable answers, and `fire(occurred_at=...)`. Under
`False`:

- an **aware** value is converted to `TIME_ZONE` local time, never read off as
  UTC;
- a **naive** value is local time, as everything else in the project is, so
  `NotYet` and `due=` accept one rather than refusing it as they do under
  `True`.

What the package hands back is in the same form: `DeliveryContext.due_at`,
`outbox_health().oldest_owed_at` and every timestamp on the rows are naive local
time under `False`. Compare them with `timezone.now()`, not
`datetime.now(timezone.utc)`.

The documented `NotYet` pattern, `if moved > max(ctx.due_at, timezone.now())`,
works unchanged under either setting. Clock changes still bend naive arithmetic
under `False`, as they do for every naive value in the project; that is the
setting's trade-off, not the package's. The autumn change makes one local hour
ambiguous. The spring change skips one, so a sum taken from a time before the
jump that lands at or after it is an hour short in real time. A lease longer
than about an hour that crosses the jump can lapse while its delivery is still
running, and the side effect can repeat. `give_up_after` subtracts the row's
`due_at`, or its event's `recorded_at`, from now in the same naive way, reads a
span crossing the jump as an hour longer than it was, and can dead-letter a row
up to an hour early. Postgres reads a local time the spring change skipped
as standard time, and an ambiguous one as the later of its two instants.

## Routing to another database

Everything that opens a transaction asks the router for the alias, rather than
hardcoding `default`. Give the event log its own database and the guarantee still
holds:

```python
class EventRouter:
    def db_for_write(self, model, **hints):
        if model._meta.app_label == "django_domain_events":
            return "events"
        return None
```

!!! warning
    A router that sends events elsewhere while your business tables stay on
    `default` **breaks the core guarantee**: the two writes are then in different
    transactions, which is the dual-write gap again. Route the event log with the
    data it describes, or accept that the atomicity is gone.
