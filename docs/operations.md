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
```

Run as many as you like on Postgres. On SQLite, run exactly one.

### `LISTEN` / `NOTIFY`

On Postgres the relay waits on a notification rather than polling, so an event
fired a moment ago is delivered in milliseconds instead of on the next tick.
`fire()` notifies after commit. The poll interval remains the floor, so a missed
notification costs latency and never a delivery.

`notify_relay()` is public, for the case where you moved rows into `pending`
yourself.

## Pruning

```bash
python manage.py prune_events                # uses RETENTION_DAYS
python manage.py prune_events --days 30
python manage.py prune_events --limit 5000
```

An outbox without a prune story becomes the largest table in the database, and
it becomes it quietly.

Only **settled** events are removed: one with a delivery still pending, failed or
claimed is still owed, and deleting it would drop work nothing recorded as lost.
An event with no delivery rows at all - suppressed, or fired with no durable
receivers - is settled by definition.

Deletes run in batches. A single statement over a year of rows holds a lock for
as long as it runs, on the table the relay is trying to claim from. Settledness
is re-checked at the delete itself, so a replay landing mid-prune does not have
its freshly reopened work cascaded away.

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

## Requeue from the dead-letter queue

```bash
python manage.py requeue_dead
python manage.py requeue_dead --receiver orders.reserve_stock
python manage.py requeue_dead --limit 100
```

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

## Handing delivery to a task queue

```python
DJANGO_DOMAIN_EVENTS = {
    "TASK_BACKEND": "django_domain_events.django_tasks_backend.DjangoTasksBackend",
}
```

or with options:

```python
DJANGO_DOMAIN_EVENTS = {
    "TASK_BACKEND": {
        "BACKEND": "django_domain_events.django_tasks_backend.DjangoTasksBackend",
        "queue_name": "events",
    },
}
```

Then declare the receiver's execution site:

```python
@receiver(OrderPlaced, site="task")
def call_the_slow_api(evt: OrderPlaced) -> None: ...
```

The relay claims the row and enqueues its id; the task runs the receiver and
acknowledges. The row is still the debt, so a lost task is still owed.

`DjangoTasksBackend` targets `django.tasks` on Django 6.0+ and falls back to the
`django_tasks` backport on 4.2-5.2.

`TaskBackend` is a `Protocol` with a single `enqueue(delivery_id)` method -
anything satisfying it works, including Celery.

!!! warning "`site="task"` needs `mode=DURABLE` and a backend"
    A non-`DURABLE` mode is refused **at the decorator** - it has no row to hand
    anywhere. A missing `TASK_BACKEND` is refused **at delivery**, and only for
    the receivers that asked for one, so configuring no backend cannot break
    receivers that never wanted it. Neither case silently runs in the relay.

## Knowing it is working

```bash
python manage.py events_status
```

See [introspection](introspection.md#is-the-outbox-keeping-up). The age of
`oldest_owed_at` is the number to alert on.

The relay logs at `WARNING` when a worker loses a delivery - either before
running it, or after finishing work whose lease had already lapsed. The second
names the receiver and suggests `lease_seconds=`, because that is the fix.

## A cron that works

```cron
*/5 * * * *  manage.py deliver_events --once
0    4 * * *  manage.py prune_events
```

With a long-running relay instead, only the prune needs a schedule.
