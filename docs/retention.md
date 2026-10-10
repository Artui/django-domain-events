# Retention

Every event is kept for `RETENTION_DAYS` (90) by default, and then deleted by
[`prune_events`](operations.md#pruning) once nothing is owed on it. An event type
can say otherwise on its declaration:

```python
from datetime import timedelta

from django_domain_events import Retention, event


@event(retention=timedelta(days=7))
@dataclass(frozen=True, slots=True)
class PasswordResetRequested: ...


@event(retention=Retention.SUCCEEDED)
@dataclass(frozen=True, slots=True)
class NewsletterSent: ...
```

!!! warning "Nothing deletes without a prune"
    A retention policy is carried out by `prune_events` and by nothing else. A
    long-running relay runs it for you whenever it is idle, so
    [`deliver_events`](operations.md#running-the-relay) deletes consumed events
    within about a minute with no cron line. A project that delivers with
    `deliver_events --once` from a schedule, or that turns the relay's sweep off,
    keeps every event forever, whatever its declaration says, until it schedules
    `prune_events` itself. See [how the prune runs](#how-the-prune-runs).

## The three forms

| Declared | Deleted | Kept meanwhile |
| --- | --- | --- |
| nothing (the default) | `RETENTION_DAYS` after it was recorded, once every delivery is terminal | everything, dead letters included |
| `retention=timedelta(...)` | that long after it was recorded, once every delivery is terminal | the same, for its own window instead of the default |
| `retention=Retention.SUCCEEDED` | at the first prune after **every delivery has succeeded** | a dead or orphaned delivery keeps the whole event for `RETENTION_DAYS`, so the dead letter stays inspectable and `requeue_dead` can still reopen it |
| `retention=Retention.SETTLED` | at the first prune after **every delivery is terminal**, dead letters included | nothing past consumption |

"Terminal" is succeeded, dead or orphaned; a delivery pending, failed or claimed is
still owed, and no form deletes an event that is still owed. An event with no
durable deliveries - nothing durable listens, or it was recorded inside
[`suppressed()`](scope.md) - is consumed as soon as it commits, so under either
`Retention` policy its row goes at the next prune, suppression reason and all.

A window of its own may be longer than `RETENTION_DAYS` as well as shorter: the
default is a default, not a ceiling. It has to be a whole number of seconds, from
one second to about 68 years, which is what the event row can record; anything
else, and a bare number, is refused when the class is declared.

## The policy is recorded with the event

`fire()` copies the declared retention onto the event row, as it copies
`max_attempts` onto each delivery. The prune reads the row and never the
registry, so:

- **changing a declaration reaches only events fired after the deploy.** An
  event recorded under `Retention.SUCCEEDED` is deleted on consumption even if
  the class has since moved to a 30-day window, or been deleted;
- **`prune_events --days` replaces only the default window.** An event recorded
  with a window of its own keeps it.

## What deleting on consumption gives up

An event deleted minutes after it was consumed takes with it everything that
reads the event table:

- **`dedupe_key`** is unique on the event row, so it protects only while the row
  exists. `fire()` raises `ValueError` when given one for an event declared with
  a `Retention` policy, rather than let a duplicate through the moment the first
  is pruned. Declare a `timedelta` window instead, as long as the key has to
  hold.
- **`replay_events` cannot replay what is gone.** There is no row to reopen.
- **Under `Retention.SETTLED`, dead letters go too.** `requeue_dead`, the dead
  counts in [`outbox_health()`](introspection.md#is-the-outbox-keeping-up) and
  the admin never see them. `Retention.SUCCEEDED` is the policy that keeps them.
- **The admin and the event log** show the event only until the next prune.

[`quiet_receivers()`](introspection.md#quiet-receivers) is not on that list: the
prune records each receiver's newest success among the rows it deletes, in the
same transaction, and the report takes the later of that and the rows still
there.

## How the prune runs

A consumed event is deleted at the next prune, so how often the prune runs is the
policy's latency. A relay that finds nothing to claim runs it, at most once per
`RELAY_PRUNE_SECONDS` (60), the first one an interval after the relay starts. That
makes "deleted as soon as consumed" true within about a minute of a sendout's
last delivery, with nothing to schedule:

- **Every long-running relay sweeps**, whatever its `--lane`, so a deployment
  of lane relays prunes too. Several relays each sweep once per interval; that is
  safe because the prune re-checks at the delete that an event is still due, and
  it is cheap for the reason below.
- **A relay with work does not sweep.** The sweep runs from an idle pass, so a
  saturated relay defers it until it catches up, and a fleet that is never idle
  never prunes. If that is your deployment, schedule `prune_events` as well.
- **`deliver_events --once` never sweeps.** It is a schedule's job, and the
  schedule can carry the prune.
- **A sweep that fails is logged and the relay carries on**, without trying
  again before the next interval.
- A sweep is not interrupted by a stop request: the relay stops when it
  returns, so the first sweep over a large backlog of already-due events is
  better run by hand (`prune_events`) than by the first relay to start.

Set [`RELAY_PRUNE`](settings.md#relay_prune) to `False` to turn it off when
`prune_events` runs from a schedule of its own, and
[`RELAY_PRUNE_SECONDS`](settings.md#relay_prune_seconds) to move the interval.

A sweep with nothing to delete runs four queries, and none of them reads
history. The ordinary window is one index range on `recorded_at`. Each policy,
and the windows of their own, read a partial index holding only their own
events still alive, and check each of those against a partial index of the
delivery rows that have not succeeded - owed rows and dead letters, never the
delivered history. So the cost grows with how many events with a retention of
their own are alive, and not with how much the tables have ever held:

- An event deleted on consumption leaves its index within a prune of being
  consumed, so for those it is the backlog - except that a `Retention.SUCCEEDED`
  event kept by a dead letter stays for `RETENTION_DAYS`, and every prune reads
  it.
- An event with a window of its own stays in its index for that window. The
  window is per event, so it cannot bound a range of the index: every prune
  reads every such event alive. A long window on a frequent event is the
  expensive case.

Measured on Postgres 17 (a laptop, synthetic data, single warm runs, so
directional), with 55,000 such events alive - 50,000 with a window of their
own, 5,000 `Retention.SUCCEEDED` events kept by a dead letter - and 445,000
delivery rows, a prune with nothing to delete takes about 12 ms. The windows of
their own take about 6 ms of it, 50,000 index entries read and discarded; the
dead letters about 3 ms; the ordinary window nothing measurable.

That is the price of an idle sweep, once per relay per interval: with three
relays and the default 60 seconds, about 36 ms of database time a minute for
that population, and a few milliseconds for one with few events of its own
alive. It grows with the live events that carry a retention of their own, not
with history, so a long `timedelta` window on a frequent event is what to watch,
and the interval is the lever: raise `RELAY_PRUNE_SECONDS` before turning the
sweep off.

Before the per-policy queries and the delivery index, the same prune took about
50 ms and read the delivery table in full on every run, a cost that grew with
history rather than with the events still alive.

It deletes in [batches of rows](operations.md#pruning), not of events, so a
20,000-target fan-out does not become one 20,000-row transaction.
