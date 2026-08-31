# django-domain-events

A Django domain-event log with in-process fan-out.

`fire()` records an event row inside the caller's transaction, and a relay
delivers it to registered receivers afterwards. **The event exists if and only if
the change committed.**

This is not a signals replacement: a database write per event rules out chatty
notification use. What it buys instead is a crash story, durable attribution for
who caused what, and an event log you can query.

Requires `django.contrib.auth` in `INSTALLED_APPS`: the event row carries a
nullable foreign key to `AUTH_USER_MODEL`.

## Attribution

```python
with attributed(actor=request.user, source="checkout"):
    with transaction.atomic():
        fire(OrderPlaced(...))
```

Every event fired inside the block records who caused it, in what scope, and
which chain it belongs to. `suppressed(EventClass, reason=...)` records without
delivering. `propagate_scope(fn)` carries the scope into a thread you start
yourself, which otherwise begins with an empty context.

See the [README](https://github.com/Artui/django-domain-events#readme) for the
quickstart and the delivery modes. The API is not yet stable.
