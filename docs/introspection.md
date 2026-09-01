# Introspection

The second reason this package exists. Signals structurally cannot answer any of
these questions, and all of them are cheap once a registry and a table exist.

## The catalogue

Every declared event, its payload shape, and everything listening to it -
generated from the declarations rather than maintained beside them, so it cannot
drift.

```bash
python manage.py export_catalogue                          # Markdown to stdout
python manage.py export_catalogue --format json
python manage.py export_catalogue --output docs/events.md
```

```python
from django_domain_events import catalogue, render_catalogue

document = render_catalogue(catalogue(), format="json")
```

Both formats exist because they answer different questions. Markdown is read by
a person onboarding onto a codebase. JSON is diffed by a pipeline that wants to
fail a pull request for removing a field other teams consume:

```bash
python manage.py export_catalogue --format json --output events.json
git diff --exit-code events.json
```

Everything is sorted by name and every document ends in exactly one newline, so
import order is not a difference and a checked-in catalogue produces a clean
diff.

!!! tip "An event nothing listens to is the finding"
    The Markdown says **"Nothing listens to this event."** rather than leaving
    the section empty. That is the usual reason to read a catalogue at all.

Building a catalogue never runs consumer code: a `default_factory` is **named**,
not called.

## Who listens to what

```python
from django_domain_events import listens_for, what_listens_to

what_listens_to(OrderPlaced)  # [RegisteredReceiver, ...] sorted by key
listens_for("orders.reserve_stock")  # RegisteredEvent | None
```

`what_listens_to` spans every mode, not only the durable ones: "who reacts to
this" is a question about the code, and an inline receiver is as much a reaction
as a queued one.

`listens_for` is the direction an operator actually needs. A dead-letter row
names a receiver key, and the next question is always what it was supposed to be
receiving. `None` covers two cases that look identical from a delivery row - a
key never declared, and one whose event class was deleted out from under it.

## Quiet receivers

```bash
python manage.py quiet_receivers
python manage.py quiet_receivers --days 30
```

```python
from datetime import timedelta

quiet_receivers(within=timedelta(days=30))
```

> "This receiver has not received anything in ninety days" is a **query** here,
> not a guess.

Driven by the registry rather than by the table, so a receiver that has **never**
received anything appears - which is the answer worth having, and exactly the one
a query over delivery rows alone cannot produce, because there is no row to find.

Only `DURABLE` receivers are reported: the others leave no rows, so they have no
history to be quiet about, and listing them as silent forever would train the
reader to ignore the output.

Only a **succeeded** delivery counts. The question is whether the receiver did
its work, not whether the relay tried - a row stuck failing for a month is
exactly the case this must catch.

The window defaults to `RETENTION_DAYS`, which is not a coincidence of numbers:
past that point the prune has deleted the evidence, so "quiet for longer than
retention" is the longest answer this can honestly give.

The success time is read off `succeeded_at`, which is written on success and
never cleared - unlike `completed_at`, which replay and requeue clear because a
reopened row has not settled again. Reading the cleared column would tell an
operator who had just replayed yesterday's events that the receiver they were
re-running had never run at all.

## Is the outbox keeping up?

```bash
python manage.py events_status
python manage.py events_status --format json
```

```python
from django_domain_events import outbox_health

health = outbox_health()
```

`quiet_receivers()` answers whether a *receiver* is running; this answers
whether the *queue* is draining. They fail differently, and both failures are
real: a relay that has been down an hour has every receiver quiet and a backlog
climbing, while one wedged receiver has a backlog and everything else fine.

| Field | Means |
| --- | --- |
| `owed` | Deliveries not yet settled - pending, failed or claimed |
| `claimed` | Currently leased by a worker |
| `dead` | Dead-lettered. **Not** counted as owed |
| `oldest_owed_at` | When the oldest still-owed delivery's event was **recorded**; `None` when nothing is owed |
| `lapsed_leases` | Claimed rows whose lease has expired |
| `receivers` | Per-receiver backlog, worst first, omitting the ones with nothing owed or dead. Each carries its own `oldest_owed_at` |

**The age of `oldest_owed_at` is the number to alert on**, and it is measured
from when the work *arrived*, not from when it next becomes due. That
distinction is the whole point: the backoff pushes a failed delivery's
`available_at` into the future on every attempt, so an alert written against
that column reads a **negative** age exactly while a receiver is failing.

It rises monotonically for as long as work sits undone - a relay that is down,
a receiver that is failing its way through its retries, or a queue simply longer
than the workers can drain.

!!! warning "It does not cover a receiver failing all the way to dead"
    A dead-lettered row leaves the owed set, so the backlog age drops back to
    `None` while nothing at all is being delivered. A dead letter is
    deliberately not owed - the relay will never pick it up again on its own,
    and counting it would leave a backlog alert firing forever after one bad
    deploy. **Alert on `dead` as well.** One number does not cover both.

Steady non-zero `lapsed_leases` means workers are dying mid-delivery, or a
receiver outruns its lease and has its work thrown away every time - see
[`lease_seconds`](declaring.md#receivers).

`owed` means "not terminal", which is what the prune settles by and a superset
of what the relay can claim at any given moment: a row inside its backoff window
is owed and not yet claimable. The superset is the useful side - this cannot
report an empty queue while work is still outstanding.

## System checks

Run with `python manage.py check`.

| id | Level | Fires when |
| --- | --- | --- |
| `E001` | Error | A receiver listens for a class that was never `@event`-decorated |
| `E002` | Error | The configured `CODEC` cannot be imported |
| `W001` | Warning | Deliveries are owed to a receiver key the registry no longer has |
| `W002` | Warning | Deliveries are owed for an event name the registry no longer has |

`E001` matters because nothing else would ever say so: the event cannot be fired,
so there is no failure to observe, only silence.

`E002` runs at startup because a codec is imported lazily - without it, the first
symptom of a missing extra is a delivery failing in a worker.

`W002` catches **a renamed event**, which `W001` cannot see: the receivers keep
their keys, so nothing looks orphaned, while every row written under the old name
now decodes to nothing and spends one attempt budget at a time finding out. Fix
it by pinning the old identity with `@event(name=...)` on the class that replaced
it.

Both warnings are limited to work **still owed**, and both mean the same thing
by it: any status the relay would still claim - which includes `claimed`, since
a worker that dies leaves the row claimed under a lapsed lease. A settled row
naming a retired event is history, and warning about history on every `check`
run teaches the reader to skip the output.

## The admin

Add `django.contrib.admin` and both models appear, read-only, with actions.

- **Event records** - filter by name and date, see how many deliveries are still
  owed per event, and **Replay selected events**.
- **Delivery records** - the dead-letter queue, filterable by status and
  receiver, with **Requeue selected dead deliveries**.

Both are read-only, and that is deliberate: the one guarantee this package sells
is that a row exists if and only if the change committed, and a form that can
write one is a way to break it. Editing `status` by hand is how a claimed row
gets handed to a second worker.

Deleting is refused too. It would cascade owed deliveries away with no record
that anything was lost; [`prune_events`](operations.md#pruning) is the supported
route, because it re-checks settledness at the delete itself.

The requeue action goes through `requeue_dead()` rather than a bulk update, so it
resets the attempt budget, clears the stale lease and error, and wakes the relay.
It reports what it skipped: a mixed selection requeues only the dead rows.

!!! warning "Both actions need the model's **change** permission"
    Django offers an action with no declared permission to anyone who can reach
    the changelist, and `has_change_permission` gates the form alone. Replay
    re-runs every durable receiver - re-sent emails, re-called webhooks - so a
    view-only grant must not carry it. Give `change_eventrecord` /
    `change_deliveryrecord` to whoever may run them; the edit form stays refused
    either way.

The filters are built from the **registry**, not from the table: a
`SELECT DISTINCT name` over an event log is a full scan on every page load, and
an event declared but never fired would not appear. Selecting one and seeing an
empty list is itself the finding.
