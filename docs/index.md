# django-domain-events

A Django domain-event log with in-process fan-out.

`fire()` records an event row inside the caller's transaction, and a relay
delivers it to registered receivers afterwards. **The event exists if and only if
the change committed.**

This is not a signals replacement: a database write per event rules out chatty
notification use. What it buys instead is a crash story, durable attribution for
who caused what, and an event log you can query.

See the [README](https://github.com/Artui/django-domain-events#readme) for the
quickstart and the delivery modes. The API is not yet stable.
