# Scheduled reactions

"Because this committed, that must happen at a set time afterwards": an offer
that expires 48 hours after it was made, an invoice reminder a fortnight after
the invoice, a newsletter sent at the time chosen when it was scheduled. The
usual answer is a beat job that polls every minute, finds nothing almost every
time, keeps no record of the work it did and has no retries, dead letters or
visibility - and two overlapping runs can do the same work twice.

Here it is a receiver that runs later. `due=` on the declaration writes the
delivery row when the event fires, inside the caller's transaction, and the
relay claims it when it comes due. Everything a delivery already has - attempts,
backoff, dead letters, replay, health, the admin - applies to it unchanged, from
its due time on.

```python
from datetime import timedelta

from django.utils import timezone

from django_domain_events import DeliveryContext, NotYet, receiver


@receiver(OfferMade, key="waitlist.send_offer")
def send_offer(evt: OfferMade) -> None:
    mail_offer(evt.offer_id)


@receiver(OfferMade, key="waitlist.expire_offer", due=timedelta(hours=48), takes_context=True)
def expire_offer(evt: OfferMade, ctx: DeliveryContext) -> None:
    offer = Offer.objects.get(pk=evt.offer_id)
    if offer.accepted:
        return  # the world moved on: nothing to do, and that is a success
    if offer.expires_at > max(ctx.due_at, timezone.now()):
        raise NotYet(offer.expires_at, reason="the offer was extended")
    offer.expire()
```

The time belongs to the receiver rather than to `fire()` because one event
drives reactions at different times: the same `OfferMade` sends the offer now
and expires it in 48 hours. And it is a receiver rather than a separate
`schedule()` call because every delivery row hangs off an event, and the event
is the record of why the work exists.

## Declaring a due time

`due=` takes one of two spellings:

| Spelling | Due at |
| --- | --- |
| A `timedelta` | That long after the event's `occurred_at`, the domain's own time |
| A callable of the event | The aware `datetime` it returns, for a time the event itself names |

```python
@receiver(NewsletterScheduled, due=lambda evt: evt.send_at)
def send_newsletter(evt: NewsletterScheduled) -> None: ...
```

The row is written with its `available_at` and its `due_at` at the due time, so
the relay claims it then and not before.

**A due time is never earlier than the row.** One already past - an event fired
with an old `occurred_at`, say, in a backfill - is clamped to the moment the row
is written, the event's `recorded_at`, and is owed from then. `due_at` is when a
row became owed, which cannot be before it existed: without the clamp a
backfilled event would read as a backlog that old the instant its row was
written, and a [`give_up_after`](delivery.md#give_up_after-keeps-every-delivery-ending)
measured from it would dead-letter its first deferral.

!!! danger "A callable runs inside the caller's transaction"
    As a [`targets=`](declaring.md#fan-out-one-delivery-per-target) callable
    does: at fire time, beside the event row, seeing the business change's own
    uncommitted rows. **If it raises, or returns anything but an aware
    `datetime`, `fire()` raises** and the caller's change rolls back. A naive
    datetime is refused rather than read as UTC or as local time, either of
    which is wrong for somebody by hours - unless the project runs with
    [`USE_TZ = False`](settings.md#use_tz-false), where naive is local time by
    the project's own convention and is accepted. A reaction that silently
    never happened is the failure this package exists to rule out.

!!! note "The due time is frozen at fire time"
    Like `max_attempts`, it is resolved once, when the event fires, and written
    onto the row. A deploy that changes the delay reaches the next event, not
    the rows already waiting. A replay resolves it afresh.

### What is refused at the decorator

| Declared with `due=` | Refused because |
| --- | --- |
| `mode=INLINE` or `ON_COMMIT` | There is no delivery row to wait in |
| `eager=True` | An eager attempt runs at commit, and a scheduled delivery is not due then |
| `targets=` | A fan-out resolved at fire time would keep that recipient set until the row came due, weeks later, while a replay resolves targets when it runs - one receiver meaning two things |
| A zero or negative `timedelta` | A delay that is not in the future is no schedule |
| Anything but a `timedelta` or a callable | Refused with `TypeError`, rather than at the first fire |

**A scheduled fan-out is a follow-up event.** Declare the scheduled receiver
without `targets=`, and have it fire an event of its own when it runs; a
receiver of *that* event declares `targets=`, which are resolved when the work
is due rather than when it was scheduled.

```python
@receiver(ActivityPlanned, due=lambda evt: evt.starts_at - timedelta(days=1))
def remind_attendees(evt: ActivityPlanned) -> None:
    fire(ActivityReminderDue(activity_id=evt.activity_id))


@receiver(ActivityReminderDue, targets=attendees_now, takes_context=True)
def remind(evt: ActivityReminderDue, ctx: DeliveryContext) -> None: ...
```

### Unchanged by a due time

- **`site="task"` works as it does for every receiver.** The relay claims the
  row at its due time and then hands it to the task backend. Nothing relies on
  a queue's own delayed delivery, so the schedule survives a queue that loses
  messages and is visible where every other delivery is.
- **Attempts, backoff and dead letters start at the due time.** A receiver
  failing after it came due retries on its curve and dead-letters on its budget,
  as any other does.
- **`give_up_after` measures from the due time**, which is when the row became
  owed.
- **Lanes, leases and `on_failure`** are what the declaration says.

## When the work turns out to be moot

The world moves on between the fire and the due time: the offer was accepted,
the invoice was paid, the activity was cancelled. There are two cases, and they
have different answers.

**Found moot when it runs.** The receiver re-reads current state, finds nothing
to do and **returns, which is an ordinary success**. "Nothing to do" is not a
different outcome from the relay's point of view - the delivery ran and its work
is done. This is the case nobody announced, and it is why a scheduled receiver
always re-checks.

**Known moot ahead of time.** The code that accepts the offer knows the expiry
is pointless. Waiting for it to run and succeed records a success for work that
was never done; cancelling it is the answer, below.

## Cancelling work known to be moot

```python
from django.db import transaction
from django_domain_events import cancel_deliveries

with transaction.atomic():
    offer.accept()
    cancel_deliveries([offer.made_event_id], receiver_key="offers.expire_offer")
```

[`cancel_deliveries`](operations.md#cancel-owed-work) settles the event's owed
rows for that receiver as `cancelled`, so the relay never claims them, at the due
time or after it. Inside the transaction it commits with the change that made
the work moot, or not at all. It returns how many it cancelled and how many it
could not, as `{"cancelled": n, "in_flight": m}`.

- **A pending row** - still waiting for its due time - is cancelled.
- **A failed row** - it came due, failed, and is waiting out its backoff - is
  cancelled too: "never runs" covers its retries.
- **A claimed row is left alone.** A worker holds it and may be running the
  receiver now. It is counted in `in_flight`, and the receiver's own re-check is
  what stops it: cancelling does not replace the re-check, it saves the run.

`cancelled` is a settled status of its own, distinct from a success (the receiver
never ran) and from a dead letter (nothing failed). Health does not count it,
prune treats it as consumed under either retention policy, and
[replay](#replay) reopens it - the way back for work withdrawn by mistake.

The event id is what `fire()` returned. Keep it where the work's subject lives -
a column on the offer - or find it through the event log by name and payload,
which is a query on the event table rather than on this package.

An operator rather than the application has the same operation in two other
places: `python manage.py cancel_deliveries <event_id> --receiver <key>`, and the
admin's **Cancel selected deliveries** on the rows a status or receiver filter has
already picked out. The command is covered in
[operations](operations.md#cancel-owed-work), the admin actions in
[the admin](introspection.md#the-admin).

## When the work is not due yet: `NotYet`

The early case is the dangerous one, because it fails silently. An admin extends
an offer from 48 hours to 72; the 48-hour row runs, finds the offer still open,
and returns - which succeeds, and leaves nothing that will ever expire the
offer. Rescheduled activities, extended invoice dates and moved newsletters all
have the same shape.

`NotYet(until, reason="")` is the answer. The receiver recomputes the due time
from current state, compares it with `DeliveryContext.due_at` - the time the row
came due with - and raises when the work has moved later:

```python
if offer.expires_at > max(ctx.due_at, timezone.now()):
    raise NotYet(offer.expires_at, reason="the offer was extended")
```

The clock term is there because a late run must proceed rather than fail: a
row that runs after the moved time has nothing left to wait for, and
`NotYet` refuses a time already come, so comparing against `ctx.due_at` alone
would raise that refusal as an ordinary failure on every retry.

The relay writes `until` to the row's `available_at` and `due_at` together, sets
it back to `pending`, and does nothing else:

- **No attempt is spent**, and the next run is handed the same attempt number.
  `max_attempts` cannot end it.
- **It is not capped** by `MAX_RECEIVER_RETRY_DELAY_SECONDS` and not bounded by
  `give_up_after`. Those exist because a destination's advice about its own
  readiness cannot be trusted to be sane; this is the application's own
  declaration of when its work is due, as `due=` is.
- **It is not a failure**: `last_error` is not written and `on_failure` is not
  called.
- **It sets nothing else aside.** The rest of the batch is delivered.

On the next run `ctx.due_at` is `until`, so the same comparison now lets the
work go ahead. A direct `deliver_one()` reschedules the same way and returns
`PENDING`, and refuses a pending row whose time has not come, as it refuses one
waiting out its backoff.

`until` must be an aware `datetime` in the future when the exception is built.
A naive one is refused - under [`USE_TZ = False`](settings.md#use_tz-false) it
is local time instead, and accepted - and so is a time already come: spending no attempt, the
row would be claimed straight back and answered the same way, a loop nothing
would end. Both are refused where the exception is constructed, so a receiver
raising either has raised an ordinary `TypeError` or `ValueError`, which the
relay records as an ordinary failure, counted, with that message.

Only a durable receiver can be rescheduled: `NotYet` is read by the relay and
by nothing else. An `INLINE` receiver raising it fails the caller's transaction
like any other exception, and an `ON_COMMIT` one has it logged. `NotYet` is not
limited to receivers declared with `due=`, though: any durable receiver can
answer "not until then", and its row is scheduled from that moment.

!!! tip "Firing an event is still the cleaner reschedule"
    Where the application knows the moment a due time moves - the admin action
    extending the offer - firing an event there and reacting to it keeps the
    log honest about why the time changed. `NotYet` is what keeps the work from
    being lost when that is forgotten, so the package does not depend on every
    change that moves a due time remembering to.

### `NotYet` or `RetryAfter`?

They rest on the same machinery and answer different questions.

| | `RetryAfter(seconds)` | `NotYet(until)` |
| --- | --- | --- |
| Whose knowledge | The destination's: "come back in two minutes" | The application's: "this work is due then" |
| Takes | A delay | An absolute time |
| Spends an attempt | Yes by default; not with `counts=False` | Never |
| Capped by `MAX_RECEIVER_RETRY_DELAY_SECONDS` | Yes | No |
| Jittered | With `counts=False` | No |
| Bounded by `give_up_after` | With `counts=False` | No |
| Pauses the lane | With `counts=False` | No |
| Recorded as | `failed`, with `last_error`, telling `on_failure` | `pending`, with neither |

Use `RetryAfter` when something outside asked you to wait; use `NotYet` when the
work itself is not due.

## Replay

[`replay_events`](operations.md#replay) resolves a scheduled receiver's due time
again, through the same resolver `fire()` uses, and writes **the later of that
and now**. Replay is how a scheduled receiver is adopted - declare the expiry,
replay the offers made this week - and writing now would expire every open
offer at once. An offer whose time has already passed is expired by the replay,
owed from it. A `due` callable is called again, and raises to the operator on
the same terms as at fire time.

`requeue_dead()` makes a row due at once, as it does for every receiver: it only
touches dead rows, which were due when they died.

A [cancelled](#cancelling-work-known-to-be-moot) row is reopened like any other
settled one, at the later of its due time and now. `requeue_dead()` leaves it
alone.

## In tests

`drain_outbox()` skips the backoff, and **never skips a schedule**: a row whose
due time has not come is left alone, so a test that makes an offer does not
expire it in the same breath. Pass `now=` to move forward instead:

```python
def test_an_unanswered_offer_expires():
    offer = make_offer()
    drain_outbox()
    assert not offer_expired(offer)

    drain_outbox(now=timezone.now() + timedelta(hours=48))
    assert offer_expired(offer)
```

`now=` is the moment every claim of the call is made at - what is due, which
backoff has run out and which lease has lapsed are all read against it.
`deliver_pending(now=)` takes the same. **The receiver still runs on the real
clock**, since its acknowledgement is a fact about when it ran: compare against
`ctx.due_at`, with the real clock only as the floor that lets a late row
proceed (`max(ctx.due_at, timezone.now())`), and the receiver behaves the same
under a test's `now=` as in production, because the rows a test moves forward
to are due after the real clock.

Within one call, a row is attempted at most once if its receiver answers
`NotYet`, and handed to a [task backend](operations.md#handing-delivery-to-a-task-queue)
at most once if it declares `site="task"` - a handed-off row is not counted in
the result, whether the backend runs it there and then or later. The pass keeps
the row out of its later claims only when it was given a `now`; on the real
clock a rescheduled row's time and a handed-off row's lease are ahead of the
clock when written, so a later claim takes the row again only once that time has
really come, and nothing is excluded or read back.

## Is it keeping up?

A scheduled row is work that has not arrived. [`outbox_health()`](introspection.md#is-the-outbox-keeping-up)
counts it as **`scheduled`**, not as `owed`, and never reads the backlog age from
it: a reminder due in fourteen days would otherwise trip a five-minute backlog
alert five minutes after it was written, and hold it open for the fortnight.

Once it comes due, a row is owed like any other, and its age is measured **from
its due time** rather than from its event: a reminder failing an hour after it
came due is an hour behind, not a fortnight. `events_status` prints the
`scheduled` total and a `scheduled=` figure per receiver beside `owed=`.

The catalogue publishes a receiver's due time as `due_seconds` or
`due_callable`, with a line under its table, so a reader sees that a reaction
runs later. See [the catalogue](introspection.md#the-catalogue).

!!! note "A scheduled row holds its event"
    A prune deletes only events with no unsettled delivery, so a row due in six
    weeks keeps its event for six weeks, whatever the event's
    [retention](retention.md). The work is owed, so that is the right answer;
    the event goes at the first prune after its last delivery settles.

!!! note "A scheduled receiver can look quiet"
    [`quiet_receivers()`](introspection.md#quiet-receivers) reports a receiver
    with no success inside its window, and a scheduled receiver has none on days
    nothing came due. Choose `within` for it from its schedule.

Nothing wakes the relay at a due time: an idle relay finds the row on its next
poll, so within `POLL_SECONDS` of it. The `NOTIFY` a `fire()` sends on commit
wakes the relay when the row is written, and it finds nothing due yet.
