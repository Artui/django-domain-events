# django-domain-events

[![CI](https://github.com/Artui/django-domain-events/workflows/tests/badge.svg)](https://github.com/Artui/django-domain-events/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Django versions](https://img.shields.io/pypi/djversions/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Docs](https://img.shields.io/badge/docs-artui.github.io-blue.svg)](https://artui.github.io/django-domain-events/)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Artui/django-domain-events/gh-pages/coverage.json)](https://github.com/Artui/django-domain-events/actions/workflows/tests.yml)

A Django domain-event log with in-process fan-out.

`fire()` records a typed event to a database table inside the caller's
transaction; a relay delivers it to registered receivers afterwards,
at-least-once, with per-receiver retry and dead-lettering. **The event exists if
and only if the change committed.**

This is not a signals replacement. A database write per event rules out chatty
notification use, and buys three things signals cannot give you: a crash story,
durable attribution for who caused what, and an event log you can query.

## Install

```bash
pip install django-domain-events
```

Nested event payloads need the decode half of the codec:

```bash
pip install "django-domain-events[dacite]"
```

## Quickstart

Declare an event and something that listens for it:

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

Fire it inside the transaction that makes the change:

```python
with transaction.atomic():
    order = Order.objects.create(...)
    fire(OrderPlaced(order_id=order.id, total_cents=order.total_cents))
```

The event row and one delivery row per durable receiver are written in that same
transaction. Deliver what is owed with `python manage.py deliver_events --once`.

In tests, `drain_outbox()` runs the real delivery path to completion, and
`assert_fired(OrderPlaced, times=1)` reads the log rather than a mock.

## Delivery modes

Two independent knobs, not one enum. Timing is what a receiver promises about the
transaction; where its code runs is a separate question, and only meaningful for
`DURABLE`.

| Mode | Runs | Can veto | Recoverable |
| --- | --- | --- | --- |
| `INLINE` | inside the transaction | yes, by raising | not needed: its failure is a rollback |
| `ON_COMMIT` | after commit, in the firing process | no | no |
| `DURABLE` (default) | after commit, at-least-once, retried | no | yes |

For a receiver that touches only this database, the work and the acknowledgement
commit together, so delivery is *effectively once*: the duplicate an
at-least-once system owes you cannot be observed. Receivers with side effects
outside the database are at-least-once, as promised.

## Status

Early development. The API is not stable and the package is not yet usable;
see the changelog for what has landed.
