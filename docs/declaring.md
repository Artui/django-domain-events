# Declaring events and receivers

## Events

An event is a **frozen dataclass** decorated with `@event`.

```python
@event
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    order_id: int
    total_cents: int
```

Frozen is enforced, not suggested: at-least-once delivery hands a *different*
instance to every attempt, so a receiver mutating one writes to a copy and the
mutation quietly disappears. The decorator raises rather than let that happen.

The default name is `<app_label>.<ClassName>`. Pin it when renaming the class
would strand rows written under the old name:

```python
@event(name="orders.OrderPlaced", version=2)
@dataclass(frozen=True, slots=True)
class OrderSubmitted: ...
```

!!! warning "The name is what rows are written under"
    Renaming the class without pinning the name leaves every unfinished row
    naming something the registry no longer has. The
    [`W002` check](introspection.md#system-checks) exists to catch exactly that,
    because nothing else would: the receivers keep their keys, so nothing looks
    orphaned.

## Receivers

```python
@receiver(OrderPlaced, mode=DURABLE, max_attempts=5)
def reserve_stock(evt: OrderPlaced) -> None: ...
```

| Argument | Default | Means |
| --- | --- | --- |
| `mode` | `DURABLE` | Timing and guarantee. See [delivery](delivery.md). |
| `key` | `<app_label>.<func_name>` | The identity delivery rows address. |
| `max_attempts` | `5` | Copied onto each row **at fire time**. |
| `eager` | `False` | Also attempt immediately after commit, relay as fallback. |
| `site` | `"relay"` | Where the code runs. `"task"` hands it to a task backend. |
| `lease_seconds` | `None` | Override `LEASE_SECONDS` for a receiver that runs long. |
| `takes_context` | `False` | Receive a second `DeliveryContext` argument. |
| `on_failure` | `None` | Called after a failed attempt is recorded. See [delivery](delivery.md#failure). |
| `targets` | `None` | One delivery per target this returns. See [fan-out](#fan-out-one-delivery-per-target). |

`takes_context` is the spelling `django.tasks.task` uses for the same idea. The
overloads make a type checker enforce the arity it implies, so declaring one and
writing the other fails at the decorator rather than in the relay hours later.

```python
@receiver(OrderPlaced, takes_context=True)
def audit(evt: OrderPlaced, ctx: DeliveryContext) -> None:
    log.info("attempt %s of %s", ctx.attempt, ctx.event_name)
```

!!! note "A long receiver needs `lease_seconds`, not a heartbeat"
    A receiver still working when its lease lapses has its row taken by another
    worker and its own work rolled back - correct, and entirely wasted. It
    cannot extend the lease itself: it runs inside the transaction carrying its
    acknowledgement, so nothing it writes is visible until it has finished.
    Declare the lease it needs and the relay publishes it before the receiver
    starts.

!!! note "`max_attempts` is frozen at fire time"
    It is copied onto the delivery row when the event is fired, so lowering it
    later cannot retroactively dead-letter rows already in flight.

## Every event: `AnyEvent`

A receiver declared for `AnyEvent` is owed **every** event fired.

```python
from django_domain_events import AnyEvent, DeliveryContext, receiver


@receiver(AnyEvent, key="bridge.forward", takes_context=True)
def forward(evt: object, ctx: DeliveryContext) -> None:
    broker.publish(ctx.event_name, evt)
```

It is for a **transport** - something that forwards events without knowing what
they are. The wildcard is matched when an event is fired, not expanded when the
receiver is declared, and that is the whole point: the alternative spelling,
walking the registry at startup and declaring one receiver per event, silently
misses every event declared by an app that loads afterwards.

!!! warning "A wildcard on its own writes a row for every event"
    Every event the system fires gains a delivery row for it, including the
    thousands nobody wants forwarded. Declare it with
    [`targets=`](#fan-out-one-delivery-per-target) and return nothing for an
    event it should skip: an empty list writes no row at all.

The catalogue lists wildcards once, in a section of their own, and
`what_listens_to(OrderPlaced)` leaves them out; `what_listens_to(AnyEvent)` lists
exactly them.

## Fan-out: one delivery per target

`targets=` makes a durable receiver owe each event to **several destinations
that live in data** - endpoint rows, tenants, subscriptions - with a delivery row
of its own for each.

```python
def endpoints_owed(evt: object, ctx: DeliveryContext) -> list[str]:
    return [
        str(pk)
        for pk in Endpoint.objects.filter(
            active=True, subscriptions__event_name=ctx.event_name
        ).values_list("pk", flat=True)
    ]


@receiver(AnyEvent, key="hooks.deliver", takes_context=True, targets=endpoints_owed)
def deliver(evt: object, ctx: DeliveryContext) -> None:
    endpoint = Endpoint.objects.get(pk=ctx.target)
    ...
```

`fire()` calls the callable with the event and a `DeliveryContext`, and writes one
row per string it returns. Each row has its own attempt count, backoff and
dead-letter, so one target that is down does not drag the others through its
retries, and the receiver is told which one it is delivering to as
`DeliveryContext.target` (and an `on_failure` hook as `DeliveryFailure.target`).

- A target returned twice is delivered **once**.
- **An empty list writes no row**, and is not an error.
- A target must be a non-empty string of at most 255 characters. Anything else
  is refused where it is returned: blank is what a receiver *without* `targets=`
  writes, and only some databases enforce the column's length.

!!! danger "The callable runs inside the caller's transaction"
    It runs at fire time, beside the event row, in whatever transaction
    `fire()` was called from - so `fire()`'s contract is its contract too. It
    sees the business change's own uncommitted rows. **If it raises, `fire()`
    raises, and the caller's change rolls back** with the event and every
    delivery row.

    That is deliberate. Swallowing the error and delivering to nobody would
    commit a change whose consequences silently never happened, which is the
    failure this package exists to rule out. Treat the callable as code in the
    middle of somebody else's write, because it is.

!!! warning "It is also a query in `fire()`'s hot path"
    Every fan-out receiver's callable is called on every event it is owed - for
    a wildcard, **every event fired** - so a callable that reads a table costs
    one query per fired event per fan-out receiver. For a transport that would
    otherwise fire a second event per destination that is a clear saving. For
    one declared carelessly, it is a cost every write in the system pays. Index
    the lookup, and return early for events you can rule out without one.

`replay_events` calls the callable again, so a replay goes to the targets that
exist at replay time; see [replay](operations.md#replay).

!!! note "Adding `targets=` to a receiver with rows still owed"
    Rows written before the receiver had a callable carry the blank target, and
    they are delivered as written - `DeliveryContext.target` is `""`. Either
    handle the blank target in the receiver or let those rows drain first.

## Where declarations live

Put them in an `events.py` module inside an installed app. The app config
autodiscovers that module name at startup, which is also how the default names
resolve - `get_containing_app_config` only answers once the app registry is
populated.

Importing the module some other way works, but a declaration that is never
imported is a receiver that never runs, with no error to read.

## Payload evolution

A row written last week is delivered or replayed today, after the event class
gained a field. The codec decides whether that explodes, and the default rule is
**additive-only, with defaults**.

| Change to the event class | Old row decodes | Because |
| --- | --- | --- |
| Field **added** with a default | yes, default filled | a missing key is not an error when the field has one |
| Field **removed** from the class | yes, extra key ignored | decoding is non-strict on purpose |
| Field **added without** a default | no | the change *was* breaking, and the error names the field |
| Type changed incompatibly | no | the error names the field, the expected type and the offending value |

The two tolerant cases are tolerated silently; the two breaking cases fail with a
sentence good enough to be the dead-letter reason. That message lands in
`last_error` verbatim, so an operator reading it knows which migration did it.

A decode failure is a **terminal state for that delivery**, never a crashed relay
loop: one undecodable row must not stop the other four thousand.

### `upgrade()` - the escape from the two breaking cases

Declare it on the event class and an older row is migrated before it is decoded:

```python
@event(name="orders.OrderPlaced", version=2)
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    order_id: int
    currency: str  # added in v2, with no default

    @staticmethod
    def upgrade(payload: dict, from_version: int) -> dict:
        return {**payload, "currency": "EUR"}
```

- It must be a **`staticmethod` or `classmethod`**, checked at the decorator. An
  ordinary method reached through the class is unbound, so the payload would
  arrive as `self` - and it would arrive there in the relay, hours later.
- It runs **only when the row is older** than the declaration. A row from the
  future is a rollback, and no forward migration helps: the code that would know
  how to read it is the code that was just removed.
- It is handed `from_version`, so one hook can cover several hops.
- It runs on **every** decode path - the relay and `assert_fired` share one - so
  a test cannot read a payload the relay would reject.
- If it raises, the delivery dead-letters with `PayloadUpgradeFailed` naming the
  class in `last_error`, rather than something unspecified going wrong between
  the row and the receiver.

## Codecs

The codec is a seam, named in settings rather than sniffed from what is
installed - a codec that picks itself decodes on one machine and raises on
another.

- `DataclassCodec` (default) - flat dataclasses, every scalar Django's JSON
  encoder handles, plus `Decimal`, `datetime`, `UUID`, `Enum` and `Literal`,
  and `list` or `tuple` of any of those.
- `django_domain_events.codecs.dacite_codec.DaciteCodec` - adds **nested**
  dataclasses on the decode side. Needs the `dacite` extra.

`tuple` is worth naming because an event is a frozen dataclass, so a `tuple` is
the sequence type that belongs in one - a `list` field is a mutable field in a
nominally immutable object. JSON has no tuple, so a tuple is written as a list
and read back as a tuple; the annotation is what decides.

```python
DJANGO_DOMAIN_EVENTS = {
    "CODEC": "django_domain_events.codecs.dacite_codec.DaciteCodec",
}
```

Two [system checks](introspection.md#system-checks) run at startup rather than
letting the problem first surface as a failed delivery in a worker: one verifies
the configured codec imports, and one verifies it can actually rebuild the events
this project declares.

The second exists because the failure it catches is asymmetric. `fire()` encodes
and commits whatever the annotation says, so an event the codec cannot decode is
recorded successfully inside the caller's transaction and then dead-letters on
every durable delivery - in the relay, in another process, possibly hours later.
Nothing before that point fails.
