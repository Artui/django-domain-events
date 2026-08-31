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
| `BATCH_SIZE` | `50` | Rows per claim, per prune batch and per requeue chunk. |
| `LEASE_SECONDS` | `300` | How long a claim is held before another worker may steal it. |
| `POLL_SECONDS` | `1.0` | Relay poll interval, and the floor under `LISTEN`/`NOTIFY`. |
| `BACKOFF_BASE_SECONDS` | `2.0` | First retry ceiling; doubles per attempt. |
| `BACKOFF_CAP_SECONDS` | `3600.0` | Ceiling the doubling stops at. |
| `RETENTION_DAYS` | `90` | Prune window, and the default quiet-receiver window. |
| `TASK_BACKEND` | `None` | Dotted path, or a `{"BACKEND": ..., **options}` mapping. |

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

`extend_lease()` covers the receiver that genuinely runs long.

### `BATCH_SIZE`

Three jobs, deliberately one number: it is "how many rows this package touches in
one statement", and the reasons to raise or lower it point the same way for all
three.

The requeue chunks on it because SQLite refuses more than 32,766 parameters in
one statement, and a dead-letter table past that is an ordinary outcome of one
bad deploy.

### `TASK_BACKEND`

A mapping rather than only a dotted path, because a backend with any constructor
options at all would otherwise be unreachable through the documented setting and
only a subclass could use it.

```python
"TASK_BACKEND": {
    "BACKEND": "django_domain_events.django_tasks_backend.DjangoTasksBackend",
    "queue_name": "events",
}
```

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
