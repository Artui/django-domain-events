# django-domain-events

A Django domain-event log with in-process fan-out.

`fire()` records an event row inside the caller's transaction, and a relay
delivers it to registered receivers afterwards. The event exists if and only if
the change committed.

This package is in early development; the API is not yet stable.
