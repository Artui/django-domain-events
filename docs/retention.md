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

!!! warning "Nothing deletes without a prune schedule"
    A retention policy is carried out by `prune_events` and by nothing else. A
    project that never runs it keeps every event forever, whatever its
    declaration says, and one that runs it daily deletes consumed events daily.
    See [scheduling](#scheduling-the-prune).

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

## Scheduling the prune

A consumed event is deleted at the next prune, so the prune's schedule is the
policy's latency. With `Retention` policies in use, run it often:

```cron
* * * * *  manage.py prune_events
```

That is cheap when there is nothing to delete: three queries, each an index range
rather than a read of the history. The ordinary window is read through
`recorded_at`; the other two through a partial index holding only the events with
a policy of their own that are still alive. Events deleted on consumption leave
it within a prune, so it stays as small as the backlog; events with a window of
their own stay in it for that window, and each prune reads all of those.

It deletes in [batches of rows](operations.md#pruning), not of events, so a
20,000-target fan-out does not become one 20,000-row transaction.
