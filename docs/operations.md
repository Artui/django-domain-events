# Operations

Everything here is a management command and a function. The functions are the
real interface; the commands are thin wrappers, so anything cron can do a
`shell_plus` session or a task can do too.

## Running the relay

```bash
python manage.py deliver_events                          # forever
python manage.py deliver_events --once                   # one pass
python manage.py deliver_events --limit 100              # cap one pass
python manage.py deliver_events --worker-id box-1        # defaults to host:pid
python manage.py deliver_events --passes 5               # stop after five
python manage.py deliver_events --lane mail              # only receivers in lane "mail"
python manage.py deliver_events --batch-size 5           # rows per claim, this process
```

Run as many as you like on Postgres. On SQLite, run exactly one.

### Lanes: a relay per kind of work

One relay delivers one row at a time. A receiver that takes a second per
delivery - an email provider, a slow partner API - holds every receiver queued
behind it for that second, and a burst of twenty thousand of them holds the
rest of the outbox for hours. A lane gives it relays of its own:

```python
@receiver(MailOrderQueued, lane="mail", takes_context=True, targets=recipients)
def send_mail(evt: MailOrderQueued, ctx: DeliveryContext) -> None: ...
```

```bash
python manage.py deliver_events                          # the default lane
python manage.py deliver_events --lane mail --batch-size 5
```

- **A relay started without `--lane` serves the default lane**: every receiver
  not declared in a named one, and every row whose receiver no longer exists,
  which it records orphaned. So the default relay stays out of `mail` without
  being told about it, and a slow receiver added next month is isolated by
  its own declaration rather than by an edit to every deployment manifest.
- **Membership is read from the declarations at every claim**, not stored on
  the row. Moving a receiver to another lane moves the deliveries it is still
  owed with it, and a receiver removed from a lane falls back to the default
  one.
- **A lane nobody declared is refused at start.** A relay for a misspelt lane
  would otherwise claim nothing, forever, and look healthy doing it.
- **`--once` follows the same rule**, so a cron running `deliver_events --once`
  beside a mail relay stays out of the mail lane. Called from code,
  `deliver_pending()` delivers every lane unless given one, and
  `drain_outbox()` always delivers every lane; `run_relay(lane=None)` serves
  every lane from a single relay, for development.
- **`--batch-size` sizes this process's claims** in place of `BATCH_SIZE`, which
  also sizes prune batches and requeue chunks. Every row of a batch is claimed
  under one lease, and a row still waiting its turn when the lease lapses is
  taken by another relay, so a slow lane wants a batch it can work through
  inside the lease its claim stamps: see
  [the lease a claim stamps](#the-lease-a-claim-stamps). With `--once` it
  cannot be combined with `--limit`, which is one claim of exactly that many
  rows; the pair is refused rather than the batch silently ignored.
- **An `eager=True` attempt still runs in the firing process**, whatever the
  lane: the lane decides which relay picks up what eager delivery did not
  finish.

**Concurrency is the number of processes.** Deliveries are serial within a
relay, so a lane that has to go faster runs more relays with the same `--lane`,
and they share its rows through the same skipped locks as any other relays. At
0.1 to 1 second per send, one process delivers 1 to 10 a second; a provider
allowing 18 a second takes somewhere between two and eighteen mail relays. The
package does not limit the rate across them - it is not a distributed rate
limiter - so the number of processes is the limit. A destination that throttles
anyway costs attempts unless the receiver
[defers without counting](delivery.md#a-deferral-that-spends-no-attempt), which
also pauses the lane in the relay that heard it and hands back the rest of its
batch.

**There is no priority inside a lane.** A lane is one queue, claimed strictly in
order of `available_at` and then id: the oldest owed row goes first, whatever
its receiver, its event or how urgent the work is to you. A password-reset mail
fired behind a newsletter of fifty thousand waits for the newsletter. Nor is
there fairness between events or receivers sharing a lane - one event fanning
out to every subscriber holds the lane until its rows are delivered. The
answer is another lane: give each class of sending that must not wait for
another its own `lane=`, and run relays for each.

A deployment with a mail lane, as Kubernetes Deployments:

```yaml
# deliver-events: the default lane
command: ["python", "manage.py", "deliver_events"]
replicas: 2
---
# deliver-events-mail: its own lane, sized to the provider's rate
command: ["python", "manage.py", "deliver_events", "--lane", "mail", "--batch-size", "5"]
replicas: 4
```

### The lease a claim stamps

Each delivery needs its receiver's lease: its `lease_seconds=`, or
`LEASE_SECONDS` where it declares none. A claim stamps one lease across every
row it takes, so it stamps the lease its lane needs, with a fixed 10 percent
of headroom:

| What the claim takes | It stamps |
| --- | --- |
| A lane whose durable receivers all need the same lease, s, no longer than `LEASE_SECONDS` | s x 1.1 |
| A lane whose receivers all need the same lease, longer than `LEASE_SECONDS` | `LEASE_SECONDS` x 1.1 |
| A lane mixing leases, or every lane at once (`lane=None`) | `LEASE_SECONDS` x 1.1 |
| The default lane | as above, over the receivers declared in no named lane |
| The `eager=True` attempt after commit | as above, over every eager receiver |

So a claim never stamps more than `LEASE_SECONDS` x 1.1. A lane of receivers
declaring a longer lease, `lease_seconds=1800` say, is claimed as it always
was, and each row is extended to its own lease when it starts: a relay killed
holding such a batch leaves its unstarted rows for 330 seconds, not 1980.

Before it runs a row, the relay extends the row's lease to the receiver's own,
unless the lease the claim stamped still is that: between s and s x 1.1 from
now, on the relay's clock, and still naming this relay as the owner. A row
started within s x 0.1 of its claim therefore needs no extension. On Postgres
the extension is a write to an indexed column and never a HOT update, so each
row that skips it costs one non-HOT update fewer. A row whose receiver needs a
different lease from the one its claim stamped is extended as before: a
60-second receiver in a mixed lane holds 60 seconds, not 330. So is every row
claimed on a moved clock, `run_relay(now=...)` or `drain_outbox(now=...)`,
when the clock was moved forward, or back by more than a tenth of the lease:
either puts the stamped lease outside the band on the real clock. A clock
moved back by less leaves it inside, and the row starts without the extension,
under a lease still at least its receiver's own.

What it means for a lane of receivers that all declare `lease_seconds=60`,
when `LEASE_SECONDS` is the default 300:

- **A crashed relay's unstarted rows are claimable again after 66 seconds**,
  where every row but the one in flight used to wait out `LEASE_SECONDS`. The
  default lane with no overrides goes from 300 seconds to 330.
- **A row in flight may hold up to 66 seconds** rather than 60.
- **The batch has to start inside 66 seconds, not inside `LEASE_SECONDS`**:
  batch size times seconds per delivery stays under s x 1.1. That binds every
  claim of the lane, not only a relay's: a `deliver_events --once --limit`
  claim of that many rows, a `deliver_pending()` batch, and the eager attempt,
  which claims every eager row of one `fire()` at once. Past that, the tail of
  the batch lapses while it waits its turn and another worker takes it.
  Relays with worker ids of their own never both run a row: the first finds
  the row gone when it tries to extend the lease, and logs that it lost the row
  before running it, but the relays churn over each other's batches. Two
  callers sharing one worker id cannot be told apart, so two
  `deliver_pending()` calls left on its default `worker_id` can both run the
  tail. They always could, once a claim lapsed; in such a lane it now lapses
  after 66 seconds rather than 300. Give each its own `worker_id`.
- **Every row skips the extension when the batch starts inside s x 0.1**:
  batch size at most s x 0.1 / seconds per delivery, plus one. At a second a
  send that is 7 rows for a 60-second lane, which also fits 66 rows inside
  the lease; `--batch-size 5` does both. Rows past that are extended as
  before, so a bigger batch costs writes, not correctness.

### Stopping it

`SIGTERM`, or `SIGINT` from Ctrl-C, asks the relay to stop. It finishes the
delivery it is running, gives back the rest of the batch it claimed, and exits.
A row given back keeps its place at the head of the queue and any other relay
can claim it at once, rather than when its lease lapses
([the lease its claim stamped](#the-lease-a-claim-stamps), five and a half
minutes by default). Giving a row back is expiring its lease, so until another
relay takes it, it is counted in `outbox_health().lapsed_leases`.

A second signal exits at once, with status 128 plus the signal number. The
receiver running at that moment is interrupted and its transaction rolls back.
Its row is reclaimed when the lease lapses, as a crashed worker's would be.

An `async def` receiver is the exception: its transaction rolls back, but the
coroutine runs on an event loop in another thread that a signal cannot reach,
and the process exits only once that coroutine returns. A third signal ends it
then. Give whatever the coroutine awaits a timeout.

How long a stop takes:

- the rest of the running receiver, if one is running;
- up to `POLL_SECONDS`, if the relay is idle and waiting for work;
- up to 15 seconds, if the database is down and the relay is waiting between
  claims (see below);
- however long a connection attempt in progress takes to fail. Against an
  address that drops packets rather than refusing them, that is the operating
  system's TCP timeout, often minutes, because libpq sets no `connect_timeout`
  by default. Set one in `DATABASES[...]["OPTIONS"]`, for example
  `{"connect_timeout": 5}`.

The stop applies to the relay. `deliver_events --once` does not handle signals:
it is one pass, and a signal ends it the way it ends any other command.

**`terminationGracePeriodSeconds` has to cover the longest receiver you run**,
and usually that is the one that decides. Kubernetes' default is 30 seconds. A
pod killed before its receiver finishes loses no delivery: the work rolls back
and the row is retried once its lease lapses. But a side effect outside the
database, such as an email, may already have happened, and it will happen again.

Python only installs signal handlers on the main thread. If the command is
started from any other thread, it says so on stderr and runs until `--passes`
is spent, and stopping it is up to whatever started the thread. An embedding
process can pass its own callable as `run_relay(stop=...)`. The relay reads it
before every claim and before every delivery.

### When the database goes away

The relay stays up when its database goes away. A failed claim, delivery or
wait is logged. After a database error the relay also closes its connection, so
the next query opens a fresh one. The close matters more than catching the
error. Django reconnects only a connection that has been closed, and a
long-running process never reaches the request boundary where Django checks for
a dead one. The relay spends nearly all its time waiting for work, so that is
where a failover usually finds it. Without the close, a connection that died
there would fail every later claim for as long as the process lived, while the
pod looked healthy.

A failed claim is retried after a pause. The pause starts at `POLL_SECONDS`,
doubles on each consecutive failure up to 15 seconds, and resets at the first
claim that succeeds. So the relay is working again within about 15 seconds of
the database coming back, plus however long a connection attempt takes to fail
(the `connect_timeout` above bounds that). A relay that exited instead would
wait out its supervisor's restart backoff, which reaches five minutes under
Kubernetes' `CrashLoopBackOff`.

### A receiver holds a transaction open

A `DURABLE` receiver runs inside the transaction that records its
acknowledgement, which is what makes a database-only receiver effectively once.
So anything it does outside the database, such as an HTTP call or an SMTP or SES
send, happens with that transaction open for as long as the call takes:

- **Behind pgbouncer in transaction-pooling mode**, that transaction pins a
  server connection for the whole call. A pool sized for millisecond
  transactions runs dry when every relay process holds a connection through a
  one-second send.
- **`idle_in_transaction_session_timeout`** counts the receiver's wait on the
  outside world as idle time. If a call outlasts the timeout, the server ends
  the session and the outcome is lost with it, so the row is delivered again
  even though the call may already have happened. Set the timeout above your
  slowest receiver for the role the relay connects as, for example
  `ALTER ROLE relay SET idle_in_transaction_session_timeout = '5min'`.

For a mail receiver, that shape is usually what you want. Store the provider's
message id in the same transaction, and it commits with the acknowledgement:
every row recorded as delivered then carries the id of the message that
delivered it. Pay for the open transaction by sizing the pool and the timeout,
rather than by moving the send out of the transaction.

### Side effects inside the acknowledging transaction

The open transaction is also where at-least-once shows. A receiver that calls
out does so before its acknowledgement commits, so for the length of the call
the connection sits idle in a transaction holding an outcome that is not yet
recorded. For a mail relay that is most of its life - about 70% of its time in
one measured deployment. A connection lost in that window - the relay killed
with `SIGKILL`, the database failing over, a proxy dropping the session - takes
the acknowledgement with it after the send has happened. The row is still
claimed, its lease lapses, and the next relay sends again.

That is the guarantee working as stated rather than a fault: the side effect is
at-least-once, and this is the "at least". It is also roughly what it costs,
which is the number worth planning for: about one duplicate per relay per
killed process or dropped connection, because each relay has at most one
delivery in that window. One test against four mail relays, sweeping their
connections five times in each of two waves, sent about 36 duplicates.

**Make the destination idempotent, keyed on `DeliveryContext.delivery_id`.**
It is stable across every attempt of one delivery and different for every
other, so a destination that has already seen it can answer the retry
without acting on it:

```python
@receiver(OrderPlaced, takes_context=True)
def email_receipt(evt: OrderPlaced, ctx: DeliveryContext) -> None:
    provider.send(..., idempotency_key=f"receipt-{ctx.delivery_id}")
```

Most mail and payment APIs take such a key. Without one, nothing on this side
can tell a duplicate apart afterwards: the attempt whose acknowledgement was
lost recorded nothing - no `attempts`, no `last_error`, no line in the
[failure log](delivery.md#the-failure-log) - which is exactly why it ran again.

A receiver shape that records "started" in a transaction of its own before the
call, so a retry could see the attempt was made, is not offered. It would turn
a duplicate into a delivery that has to be resolved by hand - the retry cannot
know whether the call landed either - and it costs every delivery a second
commit for a window idempotency already closes.

### `LISTEN` / `NOTIFY`

On Postgres the relay waits on a notification rather than polling, so an event
fired a moment ago is delivered in milliseconds instead of on the next tick.
`fire()` notifies after commit. The poll interval remains the floor, so a missed
notification costs latency and never a delivery.

`notify_relay()` is public, for the case where you moved rows into `pending`
yourself. It follows the same settings as `fire()`: under `WAKE = "poll"` it
sends nothing, and it is coalesced.

#### Coalescing

A process sends at most one notification per `NOTIFY_COALESCE_SECONDS` (half a
second by default) per database, however many events it fires in that time.
That loses nothing: one wake makes the relay claim everything that is due, and
the poll picks up whatever a skipped notification would have announced. The cost
is that an event fired just after a notification, and committed after the relay
had already claimed, waits for the next poll instead of being delivered at once.
Set it to `0` to send every notification.

`replay_events` and `requeue_dead` notify through the same function, so they are
coalesced the same way.

#### Turning NOTIFY off

Set `WAKE` to `"poll"` and neither side uses it: `fire()` sends nothing, and the
relay sleeps for `POLL_SECONDS` between passes instead of listening. Latency then
is `POLL_SECONDS`, up to a second at the default.

Why you might: in Postgres, a transaction that sends a `NOTIFY` takes a
database-wide lock while it commits, so commits that send one queue behind each
other. This package keeps the notification out of your business transaction and
sends one per `fire()` rather than one per row, but a `fire()` with a durable
receiver still ends in one such commit. Reports of this limiting throughput on
busy databases exist, but whether the lock wait is the cause or a symptom is
disputed upstream. A change in Postgres itself may have removed the bottleneck;
which release carries it is not established here, so do not assume yours does.
Measure before you give up the latency.

What makes polling cheap enough to lower `POLL_SECONDS`: the claim reads
indexes that hold only rows still owed, so it does not slow down as history
grows. It took 0.15 ms at 1.6 million delivery rows on Postgres 16 (a single
warm run on synthetic data on a laptop, so directional). A poll that cheap can
run often, so `POLL_SECONDS` is yours to lower.

#### Behind pgbouncer

`LISTEN` is state on a database session. Through pgbouncer in transaction-pooling
mode a relay's session is not its own between statements, so it never receives a
notification, and it polls instead without saying so. Nothing is lost, because
the poll is the floor, but the relay gets none of the benefit. Point the relay at
a direct or session-pooled connection to have `NOTIFY` wake it, or set `WAKE` to
`"poll"` and lower `POLL_SECONDS` to say what is happening.

## Pruning

```bash
python manage.py prune_events                # uses RETENTION_DAYS
python manage.py prune_events --days 30
python manage.py prune_events --limit 5000
python manage.py prune_events --batch-size 1000
```

An outbox without a prune story becomes the largest table in the database, and
it becomes it quietly. **Nothing else deletes**: an event declared to be deleted
on consumption is deleted by the next prune.

**A long-running relay runs that prune itself.** When a pass claims nothing, the
relay runs `prune_events()` with its defaults, at most once per
[`RELAY_PRUNE_SECONDS`](settings.md#relay_prune_seconds) (60), the first one an
interval after it starts. Every relay does, whatever its `--lane`; a relay that
is never idle does not; and `deliver_events --once` never does. A sweep that
fails is logged and not retried before the next interval, and a stop request
waits for at most one prune batch of a running sweep. Set
[`RELAY_PRUNE`](settings.md#relay_prune) to `False` if you schedule
`prune_events` yourself, which is also what a `--once` deployment needs; what the
sweep costs, and when to prefer the schedule, is in
[how the prune runs](retention.md#how-the-prune-runs).

An event is due when its [retention](retention.md) says so: consumed, for one
declared with a `Retention` policy; past its own window, for one declared with a
`timedelta`; past `RETENTION_DAYS` (or `--days`) for every other. `--days`
replaces only that default window.

Only **settled** events are removed: one with a delivery still pending, failed or
claimed is still owed, and deleting it would drop work nothing recorded as lost.
An event with no delivery rows at all - suppressed, or fired with no durable
receivers - is settled by definition.

Deletes run in batches of [`PRUNE_BATCH_ROWS`](settings.md#prune_batch_rows) rows
(`--batch-size` for one run), counting delivery rows and event rows alike. A
single statement over a year of rows holds a lock for as long as it runs, on the
table the relay is trying to claim from, and one fan-out event can hold tens of
thousands of rows; an event larger than a batch has its delivery rows deleted a
batch at a time and goes with the last of them. Settledness is re-checked at the
delete itself, so a replay landing mid-prune does not have its freshly reopened
work cascaded away.

Each batch records the newest success of every receiver among the rows it
deletes, so [`quiet_receivers()`](introspection.md#quiet-receivers) still knows a
receiver ran after its events are gone.

## Replay

```bash
python manage.py replay_events 41 42
python manage.py replay_events 41 --receiver orders.reserve_stock
```

The receiver set freezes at fire time, so deploying a new receiver does not hand
it a backlog of week-old events. Replay is the other half of that: an operation
somebody invokes, with a name, and not an accident of a deploy.

Two things happen, counted separately because they are different decisions:

- **reopened** - a terminal delivery runs again.
- **added** - a receiver with no row for that event gets one. It did not exist
  when the event fired, and you are choosing to give it the backlog.

A delivery still in flight is left alone. Reopening a claimed row would hand the
same work to two receivers, which is the one thing the lease exists to prevent.

Replay needs the row. An event the prune has deleted - including one
[deleted on consumption](retention.md), minutes after it was delivered - cannot
be replayed.

A [fan-out receiver](declaring.md#fan-out-one-delivery-per-target) has its
`targets` callable **called again**, so a replay goes to the targets that exist
now. A target it still returns is reopened or added like any receiver; one it no
longer returns is left exactly as it was and not counted. A target registered
after the event was fired receives it - a replay is a new delivery, not a re-run
of an old one. Calling the callable means rebuilding the event, so replaying a
fan-out receiver for a payload that no longer decodes raises, where a plain
receiver's reopened row would dead-letter in the relay.

A [scheduled receiver](scheduling.md#replay) has its due time resolved again, and
its row is reopened or added due at the later of that and now, rather than at
once: replaying this week's offers into a newly declared expiry expires each one
when it is due, and only the ones already past it straight away.

## Requeue from the dead-letter queue

```bash
python manage.py requeue_dead
python manage.py requeue_dead --receiver orders.reserve_stock
python manage.py requeue_dead --limit 100
```

A dead letter of an event declared with `Retention.SETTLED` is deleted with its
event at the next prune, so it is never here to requeue; `Retention.SUCCEEDED`
keeps it for `RETENTION_DAYS`. See [retention](retention.md).

Attempts reset to zero rather than staying spent: a row requeued at its limit
dead-letters again on the first failure, and the operator learns nothing they did
not already know.

Scoped by receiver, because the usual reason to requeue is that one downstream
was broken and now is not. From Python it is also scopeable by row:

```python
requeue_dead(delivery_ids=[41, 42])
```

`limit=0` requeues nothing. It is an operator asking for the smallest possible
blast radius, and reading it as "no limit" would give them the largest one.

## Cancel owed work

```bash
python manage.py cancel_deliveries 41 42
python manage.py cancel_deliveries 41 --receiver offers.expire_offer
```

The command prints `cancelled: n, in_flight: m` and refuses a call naming no
events, as `replay_events` does: the smallest command an operator can type is not
the widest. It takes **one receiver per call**, as the function does, and refuses
a second `--receiver` rather than keeping the last: `replay_events` repeats the
flag and uses every value, and a habit carried over from it would otherwise leave
the dropped receivers' work owed. From Python, which is where the transactional
case lives:

```python
from django_domain_events import cancel_deliveries

cancel_deliveries([event_id])
cancel_deliveries([event_id], receiver_key="offers.expire_offer")
cancel_deliveries([event_id], delivery_ids=[41, 42])
```

Withdraws the owed deliveries of these events so they never run, and settles them
as `cancelled`. It exists for [scheduled reactions](scheduling.md#cancelling-work-known-to-be-moot):
the offer was accepted before its expiry came due, the order was paid before its
reminder. It works on any owed delivery, though an immediate one is usually
delivered before anyone thinks to cancel it.

| The delivery is | Then |
| --- | --- |
| `pending` - waiting for its due time or for the relay | cancelled |
| `failed` - waiting out its backoff | cancelled: "never runs" covers its retries |
| `claimed` - a worker holds it | left to run, and counted as `in_flight` |
| `succeeded`, `dead`, `orphaned`, `cancelled` | left with its outcome |

It returns `{"cancelled": n, "in_flight": m}`. A claimed row is left alone because
the worker may be running the receiver right now, and settling the row under it
would record an outcome the receiver contradicts; the receiver re-checks current
state when it runs, as it always did. A dead letter is left alone because
cancelling it would drop it from the dead-letter counts without anyone having read
or requeued it.

`receiver_key` narrows it to one receiver, because one event usually drives an
immediate reaction and a scheduled one and only the scheduled one became moot.
`delivery_ids` narrows it to rows, and intersects with the events rather than
widening them, so a stale id list cannot reach another event's work.

Called inside a transaction, it joins it: the cancel commits with the change that
made the work moot, or not at all. A cancelled row writes `completed_at` and not
`succeeded_at`, so it is no evidence for
[`quiet_receivers`](introspection.md#quiet-receivers) that a receiver works.

What else reads it:

- **Health and `events_status`** count it as neither owed, scheduled nor dead.
- **Prune** treats it as settled, and under `Retention.SUCCEEDED` as consumed: see
  [retention](retention.md#the-three-forms).
- **[Replay](#replay)** reopens it like any other settled row - the way back for
  work withdrawn by mistake. `requeue_dead()` does not touch it.
- **The admin** lists it under its own status filter, and cancels from both
  changelists: [Cancel selected deliveries](introspection.md#the-admin) withdraws
  exactly the rows picked, and the event changelist's action every owed row of the
  events picked.

On Postgres each batch locks its events first, in primary key order, as replay,
requeue and the prune do. A cancel issued while a replay of the same event is open
waits for it and then cancels what it reopened, rather than finding nothing and
letting the replay make the work owed again. A relay's claim takes no event lock,
so the write itself is conditioned on the status: a row claimed between the lock
and the write is left claimed.

## Handing delivery to a task queue

```python
DJANGO_DOMAIN_EVENTS = {
    "TASK_BACKEND": "django_domain_events.delivery.django_tasks_backend.DjangoTasksBackend",
}
```

or with options:

```python
DJANGO_DOMAIN_EVENTS = {
    "TASK_BACKEND": {
        "BACKEND": "django_domain_events.delivery.django_tasks_backend.DjangoTasksBackend",
        "queue_name": "events",
    },
}
```

Then declare the receiver's execution site:

```python
@receiver(OrderPlaced, site="task")
def call_the_slow_api(evt: OrderPlaced) -> None: ...
```

The relay claims the row and enqueues its id with the claim it holds; the task
takes the row under that claim, runs the receiver and acknowledges. The row is
still the debt, so a lost task is still owed.

`DjangoTasksBackend` targets `django.tasks` on Django 6.0+ and falls back to the
`django_tasks` backport on 4.2-5.2.

### Celery

```bash
pip install "django-domain-events[celery]"
```

```python
DJANGO_DOMAIN_EVENTS = {
    "TASK_BACKEND": {
        "BACKEND": "django_domain_events.delivery.celery_backend.CeleryBackend",
        "queue": "events",  # optional; without it Celery's own routing decides
    },
}
```

The worker has to import the task, because a Celery worker runs a message by
finding its name in its own registry and `autodiscover_tasks()` only looks for a
`tasks` module in each app. Add the module to Celery's `imports`:

```python
app.conf.imports = ["django_domain_events.delivery.celery_backend"]
# or, with app.config_from_object("django.conf:settings", namespace="CELERY"):
CELERY_IMPORTS = ["django_domain_events.delivery.celery_backend"]
```

A worker without it logs the message as an unregistered task and drops it. The
row stays owed and the relay hands it off again when the lease lapses, so nothing
is lost, but nothing is delivered either.

The task is registered as `django_domain_events.deliver_delivery`, a name that
does not follow the module, so a message already on the broker still finds it
after an upgrade moves the code.

### What a task message carries

`TaskBackend` is a `Protocol` with one method,
`enqueue(delivery_id, claimed_by, claimed_at)`. The relay passes the claim by
keyword. Anything satisfying it works; the worker side has to call

```python
deliver_one(delivery_id, claimed_by=claimed_by, claimed_at=claimed_at)
```

with the three values exactly as it was given them. `claimed_at` is an ISO 8601
string, so all three are JSON and any queue can carry them unchanged.

The claim is what makes a queue that delivers more than once safe. `acks_late`
and a broker's visibility timeout both hand a worker a message whose task has
already run, and a queue can hold a message until its lease has lapsed and the
relay has handed the row to someone else. Before it runs anything, the task
*takes* the row: one conditional update, committed on its own, that succeeds only
while the row is still claimed under exactly the claim the message carries, and
moves the claim to the task's worker. A second copy, a late copy, and a copy for
a row that has since succeeded, failed or died all fail the take and do nothing.

A task worker that dies mid-delivery is recovered by the row's lease, not by the
queue redelivering: the redelivered copy finds the row taken by the worker that
died, and the relay reclaims it once the lease lapses. Size `lease_seconds=` to
cover the queue's backlog as well as the receiver's own run.

#### The lease has to outlast the queue

The lease the row holds at the hand-off is the time the message has to reach
a worker: at least the receiver's `lease_seconds=`, whether the claim stamped
it or the relay extended it there. The task extends it again before the
receiver runs, unless the queue delivered it soon enough that the lease still
is the receiver's own, so the run is covered on its own; what the hand-off
lease has to cover is the wait in the queue, which is the backlog ahead of the
message divided by the rate the workers drain it:

```
lease_seconds > messages ahead / (workers x tasks per second per worker)
```

A burst that enqueues 28,000 tasks onto workers draining 20 a second needs
about 1,400 seconds for the last of them to start. Under a 900-second lease the
rows still queued at 900 seconds lapse while their messages wait, and the
relay - which reads a lapsed lease as a dead worker - claims them again and
enqueues a second message for each. Nothing runs twice: the late first copies
fail the take and do nothing, and that is the claim working. But every one of
them is a task the workers spend dequeuing, and they arrive behind the
backlog, making it longer. In one report, a 900-second lease over a
28,000-task burst produced 12,918 stale messages, every one a no-op.

The symptom is a burst of `outbox_health().lapsed_leases` and per-receiver
`claimed` counts while the queue is long and nothing is failing, and the
relay's INFO line "carried a claim the row no longer holds" once for each
stale copy as the workers reach them. It is INFO because each one is the
outcome the take exists for; a count of them is the measure of how far the
lease fell short. The fix is the lease, sized to the largest backlog the queue
is expected to hold - not more workers alone, unless they shorten the wait
below it.

`deliver_one` refuses a row that is not owed for every caller, with or without a
claim: one that already succeeded, died or was orphaned, and one that failed and
is still waiting out its backoff. It returns `None` for those without running the
receiver.

!!! warning "`site="task"` needs `mode=DURABLE` and a backend"
    A non-`DURABLE` mode is refused **at the decorator** - it has no row to hand
    anywhere. A missing `TASK_BACKEND` is refused **at delivery**, and only for
    the receivers that asked for one, so configuring no backend cannot break
    receivers that never wanted it. Neither case silently runs in the relay.

## What a fan-out costs

A [fan-out](declaring.md#fan-out-one-delivery-per-target) writes its rows inside
`fire()`, so the write that fired the event pays for them, in its transaction.
They go in as one `INSERT` on Postgres. From Django 5.2 that statement binds
**one array per column** and reads the rows back out of `UNNEST`, where the
alternative is a `VALUES` tuple per row with a placeholder per cell, for the
driver to bind and the server to parse and plan. From 5.2.9 Django keeps the
`UNNEST` form only while no field in the insert has a placeholder hook, and the
delivery row's target digest column has none for that reason. SQLite, and
Django before 5.2, write `VALUES` for every multi-row insert.

One consumer measured `fire()` at about 130 microseconds a target while the
insert was `VALUES`. Timing `bulk_create` of delivery rows alone, on Postgres
16, gives about 55 to 75 microseconds a row as `VALUES` and about 30 as
`UNNEST`, from a thousand rows to twenty thousand.

!!! note "On MySQL, a digest write raises warning 1300"
    The hook the digest column no longer has is also what put MySQL's
    `_binary` prefix on it, so every insert or update of a digest raises
    warning 1300, as every lookup on the column always has. The stored bytes
    are unchanged. This was checked by hand against MySQL 9.6 with mysqlclient
    2.3.0 under the default strict `sql_mode`, not in CI, and not on MariaDB,
    PyMySQL, MySQL 8.x or a `utf8mb4` connection charset. A project that runs
    warnings as errors will see these, and can filter MySQLdb's warning 1300
    where it writes the `target_digest` column.

The rest of the cost is the `targets=` callable, which runs once per fired event
for every fan-out receiver owed it. Make it **one `SELECT`** returning the
strings - `values_list("pk", flat=True)`, as in the example on the declaring
page - rather than a loop over model instances, which builds an object per
target only to discard it, or a query per candidate.

## Knowing it is working

```bash
python manage.py events_status
```

See [introspection](introspection.md#is-the-outbox-keeping-up). The age of
`oldest_owed_at` is the number to alert on.

The relay logs at `WARNING` when a worker loses a delivery - either before
running it, or after finishing work whose lease had already lapsed. The second
names the receiver and suggests `lease_seconds=`, because that is the fix. It
also logs every failed attempt it records, at `WARNING`, one line each - see
[the failure log](delivery.md#the-failure-log).

## A cron that works

```cron
*/5 * * * *  manage.py deliver_events --once
0    4 * * *  manage.py prune_events
```

With a long-running relay instead, nothing needs a schedule: an idle relay
runs the prune itself, every minute by default, so an event declared to be
[deleted on consumption](retention.md) goes within about a minute. Schedule
`prune_events` as well if the relay is never idle, and instead if you set
`RELAY_PRUNE` to `False`. What a prune with nothing to delete costs is in
[how the prune runs](retention.md#how-the-prune-runs).
