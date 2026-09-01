# django-domain-events

[![CI](https://github.com/Artui/django-domain-events/workflows/tests/badge.svg)](https://github.com/Artui/django-domain-events/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Django versions](https://img.shields.io/pypi/djversions/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Docs](https://img.shields.io/badge/docs-artui.github.io-blue.svg)](https://artui.github.io/django-domain-events/)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Artui/django-domain-events/gh-pages/coverage.json)](https://github.com/Artui/django-domain-events/actions/workflows/tests.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License](https://img.shields.io/pypi/l/django-domain-events.svg)](LICENSE)

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

Add it to `INSTALLED_APPS` and migrate:

```python
INSTALLED_APPS = [..., "django.contrib.auth", "django_domain_events"]
```

`django.contrib.auth` is required: the event row carries a nullable foreign key
to `AUTH_USER_MODEL` so attribution survives, and the migration depends on it.

```bash
python manage.py migrate
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
transaction. Run the relay to deliver what is owed:

```bash
python manage.py deliver_events          # claim and deliver continuously
python manage.py deliver_events --once   # one pass, for cron or CI
```

The relay claims with `SELECT ... FOR UPDATE SKIP LOCKED` under a lease, so you
can run as many as you like: two workers never take the same row, and one that
dies without acknowledging has its rows reclaimed when the lease lapses. Failed
deliveries retry with exponential backoff and full jitter, then dead-letter.

Add `eager=True` to a receiver to also attempt it immediately after commit, in
the firing process, with the relay as the fallback.

In tests, `drain_outbox()` runs the real delivery path to completion, and
`assert_fired(OrderPlaced, times=1)` reads the log rather than a mock.

## Operations

```bash
python manage.py prune_events                 # delete settled events past the window
python manage.py replay_events 41 42          # make those events owed again
python manage.py requeue_dead --receiver k    # give dead deliveries their budget back
```

Pruning only removes *settled* events: one with a delivery still owed is kept,
because deleting it would drop work nothing recorded as lost.

On Postgres the relay waits on `LISTEN`/`NOTIFY` rather than polling, so an
event fired a moment ago is delivered in milliseconds. The poll remains the
floor.

Add `django.contrib.admin` and both tables appear, read-only, with **Replay
selected events** and **Requeue selected dead deliveries** as actions.

## Introspection

The second reason to use this rather than signals. All of it is generated from
the declarations, so none of it can drift.

```bash
python manage.py export_catalogue --format json --output events.json
python manage.py quiet_receivers --days 30
```

```python
what_listens_to(OrderPlaced)  # every receiver, sorted, across all modes
listens_for("orders.reserve_stock")  # the inverse: what a dead row was owed
quiet_receivers(within=timedelta(days=30))
```

The catalogue is every declared event, its payload schema and its receivers, as
Markdown for a person or JSON for a pipeline that fails a pull request when a
field other teams consume disappears.

`quiet_receivers()` answers *"this receiver has not received anything in ninety
days"* as a query rather than a guess - including the receivers that have never
received anything at all, which is the answer worth having and the one a query
over delivery rows alone cannot produce.

`manage.py check` adds four checks, including one for the **renamed event**: the
receivers keep their keys, so nothing looks orphaned, while every row written
under the old name now decodes to nothing.

## Attribution

```python
with attributed(actor=request.user, source="checkout"):
    with transaction.atomic():
        order = Order.objects.create(...)
        fire(OrderPlaced(order_id=order.id, total_cents=order.total_cents))
```

Every event fired inside the block records who caused it, in what scope, and
which chain it belongs to. The scope is captured at fire time and read back off
the row, so a delivery running hours later in another process still knows.

Suppress without losing the record:

```python
with suppressed(OrderPlaced, reason="historical import"):
    importer.run()  # rows written and marked, no deliveries
```

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

## Documentation

<https://artui.github.io/django-domain-events/>

## Status

Early development, and usable: the contract, the relay, ambient scope, the
operations surface and introspection have all landed. The API is not stable
until 1.0 - see the [changelog](CHANGELOG.md) for what changed in each release.
