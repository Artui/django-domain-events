# django-domain-events

A Django domain-event log with in-process fan-out.

`fire()` records a typed event to a database table **inside the caller's
transaction**; a relay delivers it to registered receivers afterwards,
at-least-once, with per-receiver retry and dead-lettering.

The one sentence everything else follows from:

> **The event exists if and only if the change committed.**

That is what closes the dual-write gap, and it is why `fire()` means "record
intent" rather than "call receivers".

## What it is not

This is **not a signals replacement**, and it should not be adopted as one. A
database write per event rules out chatty notification use. What it buys instead
is three things signals structurally cannot have:

- **A crash story.** A process that dies mid-fan-out owes the same work when it
  comes back, because the debt is a row.
- **Durable attribution.** Who caused what, recorded on the row, still readable
  hours later in another process.
- **Queryable introspection.** "Which receiver has not run since June" is a
  query here, not a guess.

## Install

```bash
pip install django-domain-events
```

```python
INSTALLED_APPS = [..., "django.contrib.auth", "django_domain_events"]
```

`django.contrib.auth` is required: the event row carries a nullable foreign key
to `AUTH_USER_MODEL`, and the migration depends on it.

```bash
python manage.py migrate
```

Nested dataclass payloads need the decode half of the codec:

```bash
pip install "django-domain-events[dacite]"
```

## Quickstart

```python
# orders/events.py
from dataclasses import dataclass

from django_domain_events import DURABLE, event, receiver


@event
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    order_id: int
    total_cents: int


@receiver(OrderPlaced, mode=DURABLE)
def reserve_stock(evt: OrderPlaced) -> None: ...
```

Declarations live in an `events.py` inside an installed app; they are
autodiscovered at startup.

```python
with transaction.atomic():
    order = Order.objects.create(...)
    fire(OrderPlaced(order_id=order.id, total_cents=order.total_cents))
```

The event row and one delivery row per durable receiver are written in that same
transaction. Then run the relay:

```bash
python manage.py deliver_events
```

## Where to go next

- [Declaring events and receivers](declaring.md) - the two decorators, payload
  rules, and what happens when a payload changes shape.
- [Delivery](delivery.md) - the two knobs, the relay, and what failure means in
  each mode.
- [Scope and attribution](scope.md) - `attributed()`, `suppressed()`, causation
  chains, and the one rule that matters for threads.
- [Operations](operations.md) - prune, replay, requeue, and running the relay
  for a year without a DBA.
- [Introspection](introspection.md) - the catalogue, the queries, the system
  checks and the admin.
- [Settings](settings.md) - every key, its default and what it costs.
- [API reference](reference.md).

## Status

Early development. The API is not stable; see the
[changelog](https://github.com/Artui/django-domain-events/blob/main/CHANGELOG.md)
for what has landed in each release.
