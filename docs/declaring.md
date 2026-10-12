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

An event is kept for `RETENTION_DAYS` unless its declaration says otherwise:
`retention=timedelta(...)` for a window of its own, or `Retention.SUCCEEDED` or
`Retention.SETTLED` to delete it, with its delivery rows, as soon as it has been
consumed. The policy is copied onto each event as it is fired. See
[retention](retention.md) for what each keeps and what deleting early gives up.

```python
@event(name="mail.NewsletterSent", retention=Retention.SUCCEEDED)
@dataclass(frozen=True, slots=True)
class NewsletterSent: ...
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
| `on_failure` | `None` | Called after a failed attempt is recorded. See [delivery](delivery.md#recording-a-failed-attempt). |
| `targets` | `None` | One delivery per target this returns. See [fan-out](#fan-out-one-delivery-per-target). |
| `backoff_base_seconds` | `None` | Override `BACKOFF_BASE_SECONDS` for this receiver's retries. See [delivery](delivery.md#a-curve-per-receiver). |
| `backoff_cap_seconds` | `None` | Override `BACKOFF_CAP_SECONDS` for this receiver's retries. |
| `lane` | `"default"` | Which relay processes claim its rows. See [lanes](operations.md#lanes-a-relay-per-kind-of-work). |
| `give_up_after` | `None` | A `timedelta`: how long a row may stay owed while it [defers without counting](delivery.md#a-deferral-that-spends-no-attempt). |
| `due` | `None` | A `timedelta` after the event occurred, or a callable of the event returning an aware `datetime`: when the delivery runs. See [scheduled reactions](scheduling.md). |

`max_attempts`, `eager`, `site="task"`, `lease_seconds`, `targets`, the two
backoff knobs, `lane`, `give_up_after` and `due` each describe a delivery row, so
a receiver declaring any of them with `mode=INLINE` or `ON_COMMIT` is refused at
the decorator: it has no row to retry, lease, fan out, schedule or claim. `due`
is also refused beside `eager=True` and beside `targets`; see
[what is refused](scheduling.md#what-is-refused-at-the-decorator).

`give_up_after` must be a positive `timedelta`; a bare number is refused,
because it has no unit. It is what ends a delivery whose receiver raises
`RetryAfter(seconds, counts=False)`, which spends no attempt: once the row has
been owed that long - since it was written, or since a replay or requeue
reopened it - the next deferral dead-letters it. A receiver that defers that way
without declaring it has its deferrals counted like any `RetryAfter`, with a
warning, so every delivery still ends.

`takes_context` is the spelling `django.tasks.task` uses for the same idea. The
overloads make a type checker enforce the arity it implies, so declaring one and
writing the other fails at the decorator rather than in the relay hours later.

```python
@receiver(OrderPlaced, takes_context=True)
def audit(evt: OrderPlaced, ctx: DeliveryContext) -> None:
    log.info("attempt %s of %s", ctx.attempt, ctx.event_name)
```

`ctx.delivery_id` is the primary key of the delivery row being attempted, for a
receiver that keeps its own record and wants to join it to the outbox's - an
email log storing the provider's message id beside the delivery that sent it,
say. It is `None` wherever no row exists: in the context a `targets=` callable
receives at fire time, and for `INLINE` and `ON_COMMIT` receivers.

`ctx.due_at` is when the delivery became due: for a receiver declared with
`due=`, the due time its row was written with - what a scheduled receiver
compares against before answering an early run with
[`NotYet`](scheduling.md#when-the-work-is-not-due-yet-notyet). For any other
row it is when the row became owed, the moment a replay or requeue reopened it
or else when its event was recorded, and it is `None` wherever `delivery_id` is.

!!! note "A long receiver needs `lease_seconds`, not a heartbeat"
    A receiver still working when its lease lapses has its row taken by another
    worker and its own work rolled back - correct, and entirely wasted. It
    cannot extend the lease itself: it runs inside the transaction carrying its
    acknowledgement, so nothing it writes is visible until it has finished.
    Declare the lease it needs and the relay publishes it before the receiver
    starts.

!!! note "`max_attempts` and `due` are frozen at fire time"
    `max_attempts` is copied onto the delivery row when the event is fired, so
    lowering it later cannot retroactively dead-letter rows already in flight.
    `due` is resolved then too and written as the row's due time, so a deploy
    changing the delay reaches the next event and not the rows already waiting.

!!! note "The curve, the lane and the bound are read live"
    `backoff_base_seconds`, `backoff_cap_seconds`, `lane` and `give_up_after`
    are the opposite of `max_attempts`: they are read from the declaration when
    they are needed - the curve when an attempt fails, the lane when a relay
    claims, the bound when a deferral arrives - and never copied onto the row.
    A deploy that changes any of them changes it for the deliveries already
    owed.

### Async receivers

A receiver may be declared `async def`, at every `mode`, and so may its
`on_failure` hook:

```python
@receiver(OrderPlaced, mode=DURABLE)
async def notify_partner(evt: OrderPlaced) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(PARTNER_URL, json={"order": evt.order_id})
    await PartnerNotice.objects.acreate(order_id=evt.order_id)
```

It runs to completion where a function would be called - in `fire()` for
`INLINE`, at commit for `ON_COMMIT`, in the relay, a task or an eager attempt
for `DURABLE` - through `asgiref.sync.async_to_sync`, which Django already
depends on. Everything else about the delivery is unchanged:

- **Its writes are in the acknowledging transaction.** The coroutine runs on an
  event loop in another thread while the calling thread waits for it, and
  Django's async ORM calls hop back to the calling thread and its connection.
  So `await Model.objects.acreate(...)` commits with the acknowledgement and is
  rolled back with it when the receiver raises, exactly as a sync receiver's
  write is.
- **It reads its verdicts the same way.** `RetryAfter`, `NotYet` and
  `PermanentFailure` raised from a coroutine reach the relay as the same
  object, and an `INLINE` coroutine's raise reaches `fire()`'s caller.
- **Causation reaches it.** An event it fires through
  `sync_to_async(fire)(...)` records it as the cause, with nothing threaded
  through.
- **A coroutine that is cancelled is a failed attempt**, retried like any other,
  rather than a row left claimed until its lease lapses.
- **A signal cannot interrupt it.** The second signal that stops a relay at
  once rolls an `async def` receiver's transaction back, but the coroutine runs
  on a loop in another thread, and the process exits only once it returns; a
  third signal ends it then. Give whatever the coroutine awaits a timeout. See
  [Stopping it](operations.md#stopping-it).

What it buys is the ability to call async libraries, **not concurrency**. The
relay still runs one delivery at a time, in one transaction, on one connection,
and each call pays for starting an event loop.

!!! warning "Each call runs on a loop of its own"
    The loop is new on every call and closed when the call returns, so nothing
    bound to a loop can be shared between calls. A module-level
    `httpx.AsyncClient` or aioboto session is bound to the first call's loop and
    fails on the second with `Event loop is closed`. Create the client inside
    the receiver, as above, or use a sync client. For the same reason, a task
    the coroutine starts with `asyncio.create_task` and does not await is
    cancelled when the loop closes (outside ASGI), so await what you start.

    Under ASGI the rule bites late. An `INLINE` or `ON_COMMIT` receiver for an
    event fired in a request served over ASGI is scheduled on the server's own
    loop instead, so a shared client can work in development and then break
    in the relay. Its writes still land in the request's transaction.

**The coroutine runs on an event loop, so sync database calls are refused in
it.** A sync ORM call raises `SynchronousOnlyOperation`, which fails the
attempt loudly rather than corrupting anything. `transaction.atomic()` is a
sync API and is refused the same way; a receiver needing a savepoint wraps a
sync function in `sync_to_async` and does the work there.

**Fire from sync code.** `fire()` writes through the sync ORM, so called inside
a running event loop it raises `SynchronousOnlyOperation` before reaching any
receiver, and `deliver_one` reads its row the same way, leaving the row
untouched and no attempt spent. With `DJANGO_ALLOW_ASYNC_UNSAFE` set they get
further, and asgiref then refuses to start a loop in a thread already running
one: an `INLINE` receiver's `RuntimeError` reaches the caller, and a durable
delivery records it as a failed attempt. From async code, wrap the call:
`await sync_to_async(fire)(event)`.

**A sync function that returns an awaitable is refused**, with a `TypeError`
naming the fix, rather than recorded as done with its body never run. That is
what a `functools.wraps` decorator around an `async def` looks like, and a
lambda returning a coroutine. An `async def` with `yield` in its body is an
async generator, which runs nothing when called, and is refused the same way. Declare the receiver `async def`, or mark the
wrapper with `asgiref.sync.markcoroutinefunction`. The refusal is an ordinary
failure, so a deploy that fixes the declaration drains the rows it failed. A
callable object whose `__call__` is `async def`, and a `functools.partial` of
an `async def`, are recognised as coroutine functions and run.

**`targets=` and `due=` stay synchronous**, and an `async def` for either is
refused at the decorator. Both run in the caller's transaction every time the
event fires, so each would start an event loop per receiver per event inside
somebody's request, for what is a database read best done with the sync ORM.
A sync wrapper returning a coroutine cannot be seen at the decorator, and fails
when the event fires: a coroutine is neither a list of targets nor a datetime.

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
- A target must be a non-empty string. Anything else is refused where it is
  returned: blank is what a receiver *without* `targets=` writes, and a
  non-string would be stored as its `str()` and never match itself on replay.

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
    the lookup, and return early for events you can rule out without one. See
    [what a fan-out costs](operations.md#what-a-fan-out-costs).

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
