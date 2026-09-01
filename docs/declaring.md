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
| `takes_context` | `False` | Receive a second `DeliveryContext` argument. |

`takes_context` is the spelling `django.tasks.task` uses for the same idea. The
overloads make a type checker enforce the arity it implies, so declaring one and
writing the other fails at the decorator rather than in the relay hours later.

```python
@receiver(OrderPlaced, takes_context=True)
def audit(evt: OrderPlaced, ctx: DeliveryContext) -> None:
    log.info("attempt %s of %s", ctx.attempt, ctx.event_name)
```

!!! note "`max_attempts` is frozen at fire time"
    It is copied onto the delivery row when the event is fired, so lowering it
    later cannot retroactively dead-letter rows already in flight.

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

## Codecs

The codec is a seam, named in settings rather than sniffed from what is
installed - a codec that picks itself decodes on one machine and raises on
another.

- `DataclassCodec` (default) - flat dataclasses, every scalar Django's JSON
  encoder handles, plus `Decimal`, `datetime`, `UUID`, `Enum` and `Literal`.
- `django_domain_events.codecs.dacite_codec.DaciteCodec` - adds **nested**
  dataclasses on the decode side. Needs the `dacite` extra.

```python
DJANGO_DOMAIN_EVENTS = {
    "CODEC": "django_domain_events.codecs.dacite_codec.DaciteCodec",
}
```

A [system check](introspection.md#system-checks) verifies the configured codec
imports at startup, rather than letting a missing extra first surface as a failed
delivery in a worker.
