# Scope and attribution

## `attributed()`

```python
with attributed(actor=request.user, source="checkout", tenant=org.id):
    with transaction.atomic():
        order = Order.objects.create(...)
        fire(OrderPlaced(order_id=order.id, total_cents=order.total_cents))
```

Every event fired inside the block records who caused it, in what scope, and
which chain it belongs to. Keyword arguments beyond the named ones land in the
row's `scope` JSON.

The actor is recorded three ways, on purpose:

| Column | Holds | Survives |
| --- | --- | --- |
| `actor` | FK to `AUTH_USER_MODEL` | joins, until the user is deleted |
| `actor_key` | `auth.User:42`, `system:relay` | everything, and covers non-users |
| `actor_label` | the display name at the time | a deleted user, and a renamed one |

A log that loses its actor when the user row is deleted is a log that lies,
which is why the FK is `SET_NULL` and the snapshot sits beside it. Plenty of
things that fire events are not users, which is why `actor_key` exists at all.

!!! warning "The scope is captured at fire time, not at delivery time"
    It is read back off the row, so a delivery running hours later in another
    process still knows. Reading a `ContextVar` inside deferred code would read
    whatever that worker happens to be in the middle of.

`AnonymousUser` is not an actor and is not recorded as one.

## Nesting

Blocks merge. An inner block adds to and overrides the outer one, rather than
replacing it (the actor is the exception, below):

```python
with attributed(source="checkout"):
    with attributed(tenant=org.id):
        fire(...)  # scope == {"source": "checkout", "tenant": org.id}
```

The actor is the exception. Naming any of `actor`, `actor_key` or `actor_label` in an
inner block replaces the whole actor, so the outer user is not recorded beside a key
from the inner block: that would describe two actors.

`current_scope()` returns the scope in force right now, as a fresh value each
call - never a shared instance, so a receiver that mutates what it is given
cannot poison the next unattributed event in the process.

## `suppressed()`

```python
with suppressed(OrderPlaced, reason="historical import"):
    importer.run()
```

The event row is still **written**, and marked with the reason. A silently
dropped event is unauditable, and that is the failure mode suppression is most
likely to cause - so the default records it and refuses to deliver it, rather
than discarding it.

A suppressed row has **no delivery rows** at all. Nothing is owed, and nothing
can dead-letter.

- The reason is required, and is at most 255 characters, the width of the
  column it is recorded in. A longer one is refused with `ValueError` as the
  block is entered, rather than by the database at the first `fire()` inside
  it - which on Postgres is an error inside your transaction, and on SQLite
  never happens at all.
- Naming no classes suppresses **everything** fired inside the block.
- Nested blocks accumulate rather than replace; the **innermost matching**
  reason is the one recorded. A library suppressing its own event type inside
  your block must not re-enable yours.
- Any matching block asking not to record wins over one that would record - the
  safer half of the disagreement.
- `record=False` discards the row entirely. It exists because a
  hundred-thousand-row import writing a hundred thousand suppressed rows is a
  surprise. It trades the audit trail for the write, so it is named rather than
  defaulted.

## Causation and correlation

`correlation_id` groups a whole chain; `causation_id` names the single event that
led to this one.

**Inside a receiver you do not have to do anything.** The package sets the
causation block around every receiver, at every execution site, so an event a
receiver fires already records its parent and stays in the same chain. A
parameter threaded through every receiver is a parameter someone forgets.

Both values are read off the parent's **row**, never from a `ContextVar`: by the
time a durable delivery runs, the block that attributed the parent has long
exited, possibly in another process.

`caused_by()` is the manual form, for linking events outside a receiver:

```python
with caused_by(parent_event_id):
    fire(StockReserved(...))
```

`causation_id` is a plain integer, not a self-referencing foreign key: retention
prunes old rows, and a cascade would make the pruner depend on the shape of a
causal graph.

## Threads and async

!!! danger "A thread you start yourself begins with an empty context"
    `contextvars` are not inherited by `threading.Thread`. A `fire()` on a
    worker thread inside an `attributed()` block records **nothing**, silently.

```python
from django_domain_events import propagate_scope

executor.submit(propagate_scope(do_work), *args)
```

`propagate_scope(fn)` captures the scope **when it is called** and runs `fn`
with those values.

!!! danger "Call it at submit time, never as a `@propagate_scope` decorator"
    It captures on call, and at decoration time - import time - there is no
    scope to capture. The decorator form silently carries nothing, which is the
    exact failure it exists to prevent.

It captures the scope's *values* rather than a `contextvars.Context`, so the
wrapper is reusable: one `Context` cannot be entered twice, and a fan-out that
submits the same wrapped callable per item would raise on the second.

Not needed for `asyncio` tasks, which inherit a copy at creation, nor across
`sync_to_async` / `async_to_sync`, which carry context both ways. It cannot help
across a **process** boundary - there the answer is the event row.

## Celery tasks

A Celery task runs in another process, so `propagate_scope()` cannot reach it,
and an event a task fires records no actor unless the scope travels with the
message. `install_celery_scope(app)` makes it travel. It needs the `[celery]`
extra and is opt-in: without the call, nothing changes.

```python
from celery import Celery
from django_domain_events import install_celery_scope

app = Celery("proj")
install_celery_scope(app)
```

Publishing a task inside `attributed()` then writes the scope into one message
header, `django_domain_events_scope`, as JSON: the actor's key, label and user
id, the scope data and the correlation id. The task runs inside that scope and
leaves it when it ends, whether it returned or raised, so an event it fires
records the publisher's actor and data and joins the publisher's chain. A task
published from inside a receiver carries that receiver's chain too.

```python
with attributed(actor=request.user, club_id=club.id):
    send_welcome_pack.delay(member.id)  # its events record request.user
```

- A task published outside any scope carries no header and fires with no actor.
- `suppressed()` and the causing event id do not travel; only the chain does.
- Installing twice is harmless.
- Under `task_always_eager` nothing is published: the task runs inline, inside
  the caller's scope already.

!!! warning "The header goes to every broker the process publishes to"
    Installing is process-wide on the publishing side. Celery's publish signal
    does not say which app published, so once any app is installed, every task
    message the process publishes inside a scope carries the header, whichever
    app sends it and whichever broker receives it. The actor's key, label and
    user id and the whole scope data travel as plain JSON to that broker and to
    whatever consumes its queue. The label is `str(user)` unless you pass
    `actor_label`, so it can be an email address. Only the apps you install
    run their tasks in the carried scope; the others' tasks ignore it, but
    their messages still carry it.

!!! note "Not for `django.tasks`"
    Django's task framework sends no signal when a task is enqueued, so there is
    nowhere to write the scope. Pass what the task needs as arguments and open
    `attributed()` inside it.
