# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- System check `W009`: a warning when the database the event rows are written
  to is a Postgres `SQL_ASCII` database. Such a database measures a `varchar`
  in bytes, while every length limit here counts characters - the refusals of
  a long receiver key, event name, `dedupe_key` and suppression reason, and the
  truncation of the actor's two strings - so a non-ASCII value inside a limit
  could still be refused by the insert with `DataError`, inside the caller's
  transaction. Under the database tag: `migrate` runs it, and
  `check --database` does. The limits themselves are unchanged, since counting
  bytes everywhere would refuse values a UTF8 database stores.
- The shop example shows the length refusals and the lease a claim stamps, and
  checks both.

### Changed
- `deliver_events --limit` without `--once` is refused with `CommandError`.
  The relay never read it, so a command line passing it to a relay was told
  nothing while the cap it asked for did not exist. A cron or process manager
  passing `--limit` to a relay must drop it, or add `--once` if a single
  capped pass was what it meant; `--passes` bounds a relay and `--batch-size`
  sizes its claims.
- The test suite runs on MySQL 8.4 in CI, as a correctness gate beside the
  Postgres jobs. The fixes below are what it found on its first run.
- The docs say what MySQL's missing partial indexes cost: Django creates none
  of the six there, and warns `models.W037` once per table.

### Fixed
- An `eager=True` receiver was never attempted at commit on MySQL, whose
  multi-row insert reports no ids: the attempt claimed a list of `None` and
  every eager row waited for the relay. The attempt now finds its rows by
  event and receiver after commit, the same way on every backend.
- `prune_events` failed with MySQL error 1093 on any event with more delivery
  rows than a batch, because the chunked delete re-checked the event's
  due-ness in a subquery reading the table it deleted from. The event is now
  locked and re-checked as a read of its own before the chunk is deleted, as
  a whole-event batch already was.
- Reversing migration `0006` failed on MySQL with a duplicate index name: its
  schema editor leaves an index on `event` alone when the swap drops the
  unique constraint that led with it, and the reversed `AlterField` then
  created the same name. The reversal now drops that index first. Forward is
  unchanged, so an install that already ran `0006` on MySQL keeps that
  index, which nothing reads.

## [0.13.0] — 2026-10-22

### Added
- One relay serves a set of lanes. `run_relay(lane=...)` takes a collection of
  lane names as well as one, and `deliver_events --lane` repeats:
  `--lane default --lane mail`. The relay visits its lanes in sorted order, one
  claim each, and claims each lane's batch immediately before delivering it,
  under that lane's own lease and batch size, so no batch burns its lease while
  another lane delivers. A lane paused after a deferral is skipped without a
  query while the others go on. One rotation is one pass, so `passes=` counts
  rotations, and a pass is idle - it waits, and may prune - only when every
  lane in it was empty or paused. Between two claims of one lane the relay
  delivers at most one batch of each other lane it serves, so a burst in one
  lane delays another by what one of its batches takes, never by the burst. A
  lane named twice is served once; an empty collection, a member that is not a
  `str` and a lane no receiver is declared in are refused at start. A single
  `lane="mail"` or `--lane mail` behaves exactly as before. Measured on
  Postgres 17, a claim of a quiet lane does not walk a burst in another: 0.07
  ms beside 15,000 owed rows, 0.78 ms with 400,000 settled rows besides, from a
  single run on synthetic data. No index was added.
- `lane_batch_sizes` on `run_relay` and `deliver_pending`, and
  `deliver_events --lane-batch-size LANE=N`, repeatable: the claim size of one
  served lane in place of `batch_size` or `--batch-size`. In a shared relay it
  is the lever on how long the other lanes wait. The command splits the value
  at its last `=`, since a lane name may contain one. A size below one and a
  lane the call does not serve are refused at start, from Python and the
  command line alike; the command also refuses a value with no `=`, an empty
  lane, a size that is not an integer, and a lane sized twice.
- `deliver_pending(lane=...)` takes the same collection, and visits the lanes in
  the same rotation until a whole rotation claims nothing.
- `OutboxHealth.lanes`: one `LaneBacklog` per lane - `lane`, `owed`,
  `scheduled`, `claimed`, `dead`, `oldest_owed_at` and `pauses`, a `LanePause`
  (`worker_id`, `paused_at`, `until`) per relay holding the lane paused. Every
  lane a relay can serve is listed, zeros included, so a dashboard keeps one
  series per lane, and so is a lane no receiver declares while a relay still
  holds it paused; worst first, ranked as `receivers` is. The rows of a deleted
  receiver count in the default lane, where they drain. It is last and
  defaulted, so constructing an `OutboxHealth` by keyword is unchanged.
  `LaneBacklog` and `LanePause` are exported from the package root.
  `events_status` prints a line per lane with a line under it per pause, and
  its JSON carries `"lanes"`, each with `oldest_owed_age_seconds` and
  `"paused"`, a list of `worker_id` and `paused_for_seconds`.
- `LanePauseRecord` and migration `0009_lanepauserecord`: the relay records
  each pause it takes - the lane, its `worker_id`, when and until when - so
  operators can see it. It deletes its record when the pause ends and when it
  returns, and every record written deletes every record whose pause has
  passed, whoever wrote it. A relay that died leaves its record behind: it
  stops showing once its `until` passes, and goes at the next deferral any
  relay records. No relay reads the table: a pause still governs only the relay that
  heard the deferral. A relay that never defers never touches it, and a write
  that fails is logged and survived. `deliver_pending()` and `drain_outbox()`
  write none. Run `migrate` before deploying: `outbox_health()` and
  `events_status` read the table, and raise until it exists, where the relay's
  own writes to it are logged and survived.
- psycopg2 is supported. Under it the relay polls rather than listening, and
  `fire()` sends no `NOTIFY`, as under `WAKE = "poll"`: the wait blocks in
  psycopg 3's `notifies()`, which psycopg2 does not have. System check `W008`
  warns when `WAKE` is `"notify"` on Postgres and Django loaded psycopg2; set
  `WAKE = "poll"` to say so, or install psycopg 3 to have the relay woken. CI
  runs the suite on Postgres under psycopg2 with psycopg 3 uninstalled.

### Changed
- `lane=None` - `run_relay(lane=None)`, and `deliver_pending()` and
  `drain_outbox()`, whose default it is - claims lane by lane in the rotation
  above, each claim under that lane's own lease, where it was one claim across
  every lane in `available_at` order under `LEASE_SECONDS` times 1.1 whenever
  the lanes' leases differed. In a project with named lanes the order across
  lanes is now the rotation, and an idle pass sends one claim query per
  declared lane rather than one. In a project that names no lane there is only
  the default lane, and the queries and their count are unchanged.
- `deliver_pending(limit=)`, `drain_outbox(limit=)` and
  `deliver_events --once --limit` cap the call's total and share it between
  the lanes with work, where the limit used to be one claim of that many rows.
  Each lane's claim takes what is left of the limit divided by the lanes still
  open in the rotation, rounded up; a lane that claims less than its share is
  drained for the rest of the call, and the others take what it left. So a lane
  with work gets at least the limit divided by the number of lanes, rounded
  down, and a burst in one lane cannot spend every cron run. A limit below the
  number of lanes gives one row each to the first lanes in sorted order. Rows
  handed back after a deferral do not count against it. With one lane this is
  one claim of the whole limit, as before.
- The refusal of `limit` with `batch_size` is reworded, from "limit=N is one
  claim of that many rows" to "limit=N sizes each claim as its lane's share of
  it", and `limit` with `lane_batch_sizes` is refused the same way. Code
  matching the old message needs the new one.
- `outbox_health()` sends four queries where it sent three: the fourth reads
  the pause records, after the three over the delivery table. A test counting
  its queries sees one more.
- `events_status` text output gains a `lanes` block between the totals and the
  per-receiver lines, which now sit under a `receivers` heading. Anything
  parsing the text by position needs updating; the JSON only gains a key.
- The 255-character refusals of a receiver key and an event name moved from
  `@receiver` and `@event` into `registry.register_receiver` and
  `registry.register_event`, so a `RegisteredReceiver` or `RegisteredEvent`
  registered directly is refused too, with the same message. A declaration
  through the decorators is refused exactly as before.
- `fire()` refuses a `dedupe_key` longer than 255 characters with `ValueError`
  before writing anything, and `suppressed()` a `reason` longer than 255 as the
  block is entered: 255 is the width of the column each is stored in. This is
  a break on SQLite, which stored either whole, so a project passing one
  there now fails at the call. On Postgres both already failed, with
  `DataError` from `fire()`'s insert inside the caller's transaction, and now
  fail earlier, with a message naming the length and the limit. A `reason`
  given with `record=False`, which is never stored, is refused all the same, on
  every backend. The actor's key
  and label keep their truncation at capture, which already holds them to
  their columns.

### Fixed
- Under psycopg2 an idle relay spun. Every idle wait raised `TypeError` from
  psycopg2's `notifies`, which is a list rather than psycopg 3's generator, and
  the relay logged it and claimed again at once: a claim query and a traceback
  per loop for as long as it was idle, under `WAKE = "notify"`, the default on
  Postgres. It now polls under psycopg2 (see Added).
- Whatever the driver, a wait that raised was followed by no sleep, so a wait
  failing at once - a server refusing every connection - turned an idle relay
  into a loop of claims. The relay now sleeps `POLL_SECONDS` in its place.
- Under psycopg2, `replay_events` counted every target that already had a row
  as added, and sent a `NOTIFY` for a replay that changed nothing: psycopg2
  reads the digest column back as a `memoryview`, which compared unequal to
  the digest computed. The rows it wrote were right. It reads the digests back
  as `bytes` now.

## [0.12.0] — 2026-10-12

### Fixed
- On Postgres with Django 5.2.9 or later, every multi-row insert of delivery
  rows - a fan-out in `fire()`, and the rows `replay_events` writes for
  targets it finds missing - was written as a `VALUES` tuple per row rather
  than Django's `UNNEST` form, which binds one array per column, at about
  twice the cost per row. Django gives that form up for a whole insert when
  any field has a placeholder hook, and the target digest column inherited
  `BinaryField`'s. The field now has none, under either name Django has used
  for it. Its class path, column type and migrations are unchanged, as is the
  SQL on SQLite and for single-row inserts, and so are the stored values. The
  one edge is a receiver key longer than its 255-character column: on Django
  5.2.9 and 6.0.0 exactly the `UNNEST` cast truncates it where `VALUES` raised
  `DataError`, as it already did on 5.2.0 to 5.2.8, and Django dropped the
  length from that cast in 5.2.10 and 6.0.1. A declaration can no longer
  produce such a key; see Changed.
- On MySQL, as the cost of the fix above, an insert or update of a target
  digest no longer carries the `_binary` prefix, so each such write raises
  warning 1300, as every lookup on the column always did. The stored bytes are
  unchanged. Checked by hand against MySQL 9.6 with mysqlclient 2.3.0 under
  the default strict `sql_mode`, not in CI; not tested on MariaDB, PyMySQL,
  MySQL 8.x or a `utf8mb4` connection charset. A project running warnings as
  errors can filter MySQLdb's warning 1300 where it writes that column.

### Added
- `AROUND_DELIVERY` setting: a dotted path to a callable that takes the
  attempt's `DeliveryContext` and returns a context manager, entered around
  every durable attempt (relay, eager attempt, `deliver_pending`,
  `drain_outbox`, `deliver_one`, and the task worker) and never around
  `INLINE` or `ON_COMMIT` receivers, the relay's hand-off of a `site="task"`
  row, or a row that never reaches its receiver. It sits outside the
  transaction carrying the acknowledgement and is exited once the outcome is
  recorded, so `on_failure` and the failure log line run inside it.
  `__exit__` receives the receiver's exception, and its return value is
  ignored. An `Exception` from `__exit__` is logged and swallowed; a
  `BaseException` propagates, and the recorded outcome stands. An exception on
  entering the hook, including a path that no longer imports, fails the
  attempt as the receiver raising would. The hook must not open a transaction,
  since one would hold the outcome's write. The path is read on every
  delivery.
- `DeliveryContext.receiver_key`: the key of the receiver the context was
  built for. It is set on every durable delivery and on the contexts that
  `INLINE` and `ON_COMMIT` receivers and `targets=` callables receive, at fire
  time and on `replay_events`. It is last and defaulted, so constructing a
  `DeliveryContext` by keyword is unchanged.
- System check `E011`: `AROUND_DELIVERY` is not a string, does not import, or
  names something not callable.
- `async def` receivers run, at every mode, through `asgiref.sync.async_to_sync`.
  Its async ORM writes land in the transaction carrying the acknowledgement and
  roll back with it. `RetryAfter`, `NotYet` and `PermanentFailure` raised from
  it are read exactly as from a function, and events it fires through
  `sync_to_async(fire)` record it as their cause. A coroutine that is cancelled
  is a failed attempt, recorded with a `RuntimeError` naming the receiver, and
  retried. An `on_failure` hook may be `async def` too. Outside ASGI each call
  runs on an event loop of its own, closed when it returns, so a task it starts
  and does not await is cancelled; under ASGI an `INLINE` or `ON_COMMIT`
  coroutine runs on the server's loop. Either way a client bound to a loop
  cannot be shared between calls. What it buys is the ability to call async
  libraries, not concurrency. A relay's second signal, which ends a sync
  receiver at once, waits for an in-flight coroutine to return; a third signal
  ends the process.

### Changed
- A receiver whose key is longer than 255 characters, whether given as `key=`
  or derived from the app label and function name, now raises `ValueError`
  where it is declared, in every mode. Declaration runs at import, so a project
  declaring one fails to start until the key is shortened. 255 is the width of
  the column delivery rows store the key in. The length was never checked:
  SQLite stores any length, so a `DURABLE` receiver with such a key worked
  there, while Postgres either truncated the key in a bulk insert (Django 5.2.0
  to 5.2.8, and 5.2.9 and 6.0.0 once the fix above lets them use `UNNEST`,
  leaving the delivery orphaned as renamed, moved or deleted) or raised `DataError` when an event fired. `INLINE` and `ON_COMMIT`
  receivers store no key and worked on every backend; they are refused too, so
  a key stays valid when its mode changes. The message names the key's length
  and the limit; set a shorter `key=`.
- An event whose name is longer than 255 characters, given as `name=` or
  derived from the app label and class name, now raises `ValueError` where it
  is declared, as a receiver key longer than its column does, above. The
  column is `EventRecord.name`. SQLite stores any length, so such an event
  worked there; on Postgres every `fire()` of it raised `DataError` from the
  insert, inside the caller's transaction. An existing declaration over the
  limit was therefore already broken on Postgres, and after upgrading it fails
  at startup instead of at the first fire. The message names the name's length
  and the limit; set a shorter `name=`.
- A sync callable that returns an awaitable, and an `async def` with `yield` in
  its body, are refused at the call with a `TypeError` naming the fix. Earlier
  versions skipped their body silently. After upgrading, a `DURABLE` one's
  deliveries fail, are retried and dead-letter after `max_attempts`; an
  `ON_COMMIT` one logs the error; an `INLINE` one raises from `fire()` and rolls
  back the caller's transaction. `targets=` and `due=` refuse an `async def` at
  the decorator with `TypeError`; both already failed at the first fire.
- Upgrade note: `async def` receivers already declared start running on
  upgrade, and an `INLINE` one can now veto by raising. `DURABLE` deliveries
  that earlier versions recorded `succeeded` for such a receiver never ran.
  `replay_events(event_ids, receiver_keys=[key])` reopens them, for events not
  yet pruned; the ids are `DeliveryRecord.objects.filter(receiver_key=key,
  status="succeeded").values_list("event_id", flat=True)`. `INLINE` and
  `ON_COMMIT` runs left no row and cannot be replayed.
- A claim leases its rows for the lease its lane needs, with 10 percent of
  headroom: `lease_seconds` times 1.1 where every durable receiver it can take
  declares the same lease, and `LEASE_SECONDS` times 1.1 otherwise, never
  more. A delivery whose claimed lease is still its receiver's own - between
  that lease and 1.1 times it from now, under this worker - starts without
  extending it, in the relay, `deliver_pending`, the eager attempt, at the
  task hand-off and in the task worker. The extension writes an indexed
  column, so on Postgres each skip is one non-HOT update fewer. Measured on
  Postgres 16 over 5,000 deliveries of a no-op receiver in batches of 50, all
  in the band: the relay went from 3 to 2 non-HOT updates and from 20.8 to
  13.3 WAL records per delivery, and the task site from 4 to 2 and from 31.0
  to 15.9; the take stays a HOT update. WAL bytes per delivery went from 1757
  to 1310 on the relay (25 percent less) and from 2597 to 1533 at the task
  site in a steady state, and from 2163 to 2010 (7 percent) and from 3043 to
  1991 just after a checkpoint. A lane mixing leases and a relay on a clock
  moved forward are unchanged. A real lane skips only the rows that start
  within a tenth of the lease of their claim, so a large batch of slow sends
  saves less.
- Upgrade note: an unstarted row of a crashed relay is claimable again after
  its lane's lease times 1.1 - 66 seconds in a lane of `lease_seconds=60`
  receivers, where it waited out `LEASE_SECONDS` - and the default lane with
  no overrides goes from 300 to 330 seconds. A lane whose receivers declare a
  lease longer than `LEASE_SECONDS` recovers in `LEASE_SECONDS` times 1.1, as
  the default lane does (330 seconds), not in its own lease times 1.1: a claim
  stamps `LEASE_SECONDS` times 1.1 at most. A lane whose receivers all
  declare a short lease now has to start a whole batch inside that lease
  times 1.1 rather than inside `LEASE_SECONDS`, and that binds every claim of
  it: a relay's batch, a `deliver_events --once --limit` claim, a
  `deliver_pending()` batch and the eager attempt. Past it, another worker
  takes the tail. Relays with worker ids of their own never both run a row:
  the first logs that it lost the row before running it, and the relays
  churn. Two `deliver_pending()` callers left on its default `worker_id`
  cannot be told apart and can both run the tail, as they always could once a
  claim lapsed, now after 66 seconds rather than 300; give each its own. See
  "The lease a claim stamps" in the operations docs.

## [0.11.2] — 2026-10-12

### Fixed
- A project on `USE_TZ = False` could not run the relay. The package took "now"
  as an aware UTC value, so comparing it with a column read back raised
  `TypeError` - every `RetryAfter(counts=False)`, a FAILED row run without a
  claim, `quiet_receivers()`, the `events_status` ages, scheduled due times and
  replay - and on SQLite every write of one failed. "Now" is
  `django.utils.timezone.now()`, and every datetime an application hands in -
  a `now=` argument, the relay's clock, `NotYet`, a `due=` answer,
  `fire(occurred_at=)` - is converted to the project's form first: under
  `USE_TZ = False` an aware value becomes `TIME_ZONE` local time, never UTC,
  and a naive `NotYet` or `due=` answer is accepted as local time rather than
  refused. The Postgres CI job now runs under each setting.
- An idle relay woke once for every notification sent while it was busy
  delivering, each wake an empty claim: psycopg holds those notifications in a
  backlog and yields them first on the next wait, and the wait returned at the
  first and left the rest there. The wait now runs psycopg's generator to its
  end, which empties the backlog, then reads once more without blocking for any
  that reached the socket meanwhile. The backlog exists from psycopg 3.2.4, so
  the development dependency's floor is raised to it; psycopg is not a runtime
  dependency, and nothing changes for an installed project.

### Added
- Every failed attempt the relay records is logged once at `WARNING`, naming
  the receiver, the delivery, the attempt, the exception's type and whether the
  row is now `failed` or `dead`, with no traceback. A deferral and the give-up
  that ends one are logged too, and so is a payload that will not decode, as
  it is dead-lettered (its receiver's `on_failure` is not told, since the
  receiver never ran). A row orphaned because its receiver is gone counts no
  attempt and is not logged as a failed attempt. As with `on_failure`, a worker whose lease lapsed
  logs nothing.
- `ReceiverBacklog.claimed`: how many of a receiver's owed rows a worker holds,
  the per-receiver half of `OutboxHealth.claimed`, from the same grouped query.
  `events_status` prints it per receiver and carries it in `--format json`.

### Changed
- A task message turned away because its row's claim has moved on - a
  redelivered copy, or one the queue held past its lease - is logged at `INFO`
  rather than `WARNING`. It is the outcome the claim exists for, and a lapsed
  backlog produces thousands of them.
- The delivery and operations pages now say that the backoff is a floor under
  a row's place in the queue, not its delay; that a lane is strictly ordered,
  with no priority or fairness inside it; how to size a `site="task"` lease to
  the queue's backlog, and what a lapsed one looks like; and why a side effect
  inside the acknowledging transaction can repeat after a lost connection,
  with `DeliveryContext.delivery_id` as the destination's idempotency key.
  The settings page says what the spring and autumn clock changes do to a
  lease and to `give_up_after` under `USE_TZ = False`.

## [0.11.1] — 2026-10-11

### Added
- `cancel_deliveries` management command: takes one or more event ids and an
  optional `--receiver`, prints `cancelled: n, in_flight: m`, and refuses a call
  naming no events. One receiver per call: a second `--receiver` is refused
  rather than silently replacing the first.
- **Cancel selected deliveries**, an admin action on the delivery changelist:
  withdraws exactly the selected rows through `cancel_deliveries()`, scoped to
  their own events, so a sibling receiver's row of the same event stays owed. The
  message counts the rows cancelled, those in flight and left to run, and those
  already settled and left alone.
- **Cancel owed deliveries of selected events**, an admin action on the event
  changelist: withdraws every receiver's owed rows of the selected events and
  reports the cancelled and in-flight counts.
- Both actions need the model's change permission, as replay and requeue do: a
  view-only user is neither offered them nor able to post them.

## [0.11.0] — 2026-10-11

### Added
- `@receiver(due=...)`: a scheduled reaction, whose delivery of an event is
  written when the event fires and runs later. A `timedelta` counts from the
  event's `occurred_at`; a callable is handed the event and returns an aware
  `datetime`, and runs inside the caller's transaction, so `fire()` raises if it
  raises or answers with anything else. A due time already past is due when the
  row is written. Refused beside a non-durable mode, `eager=True` and
  `targets=`, and for a delay that is not positive; `site="task"` is unchanged,
  handed off when the row comes due.
- `NotYet(until, reason="")`, exported from the package root: a durable receiver
  answering an early run moves its row to `until`, an aware future time, with no
  attempt spent, no cap, no `give_up_after`, no `last_error`, no `on_failure`
  call and no lane paused. `deliver_one()` returns `PENDING` for it.
- `DeliveryContext.due_at`: the time the delivery came due with, for a scheduled
  receiver to compare against before answering `NotYet`.
- `drain_outbox(now=...)` and `deliver_pending(now=...)`: the moment the call's
  claims are made at, so a test can move forward to a due time. A drain skips the
  backoff and never a due time. Within one call, a row answered with `NotYet` is
  attempted once and a `site="task"` row handed to the backend once.
- `OutboxHealth.scheduled` and `ReceiverBacklog.scheduled`: unsettled rows whose
  due time is still ahead, which `events_status` prints and its JSON carries.
- `CatalogueReceiver.due_seconds` and `due_callable`, with a line under the
  receiver's table in the Markdown catalogue.
- `claim_batch(exclude_ids=...)`, which leaves those rows out of the claim.
- The example shop sends an invoice reminder a fortnight after the invoice,
  with a moot run and a `NotYet`.
- `cancel_deliveries(event_ids, *, receiver_key=None, delivery_ids=None)`,
  exported from the package root: withdraws the pending and failed deliveries of
  those events, optionally narrowed to one receiver and to named rows (which
  intersect with the events), so they never run. It leaves a claimed row to its
  worker, leaves a settled row with its outcome, and returns
  `{"cancelled": n, "in_flight": m}`. Inside a transaction it commits with the
  caller's change. On Postgres it locks the events first, in primary key order,
  as replay, requeue and the prune do, so a cancel issued during a replay of the
  same event cancels what the replay reopened.
- `DeliveryStatus.CANCELLED` (`"cancelled"`), the settled status a cancelled
  delivery takes. The relay never claims or runs it; `outbox_health()`,
  `events_status` and the event changelist do not count it owed; `prune_events`
  treats it as settled, and as consumed under `Retention.SUCCEEDED` as well as
  `Retention.SETTLED`; `replay_events()` reopens it; `requeue_dead()` leaves it
  alone; the admin's status filter lists it. Migration `0008` changes only the
  field's choices and emits no SQL.
- The example shop cancels the open order's next reminder when its invoice is
  paid, and checks that a drain past its due time never runs it.

### Changed
- **`owed` no longer counts a row whose due time has not come**, in
  `outbox_health()`, its per-receiver backlog and `events_status`; such a row is
  `scheduled`. `oldest_owed_at` reads a row's `due_at` where it has one and its
  event's `recorded_at` otherwise, so a replayed or requeued row ages from when
  it was reopened and a scheduled one from its due time.
- **`deliver_one()` refuses a `PENDING` row whose `available_at` is ahead**, as it
  already refused a `FAILED` row waiting out its backoff.
- **`replay_events()` keeps a scheduled receiver's due time**: a reopened or
  added row is due at the later of its resolved due time and now, rather than at
  once, and a `due` callable is called again.
- **`DeliveryStatus` has a seventh member**, `CANCELLED`. Code that maps every
  status to something - a dashboard, a metrics label set - meets a value it has
  not seen once anything calls `cancel_deliveries`.
- **During a rolling deploy, 0.10 processes read the new rows with the old
  meaning**: they treat a `cancelled` row as unsettled (their prune keeps its
  event and their admin counts it owed) and a scheduled row as owed in health,
  until every process is on 0.11.

### Fixed
- The Markdown catalogue writes a duration of a million seconds or more out in
  full, where it printed a fortnight as `1.2096e+06s`.

## [0.10.0] — 2026-10-11

### Added
- `CeleryBackend` (`django_domain_events.delivery.celery_backend.CeleryBackend`),
  a task backend for projects that run Celery, behind a new `celery` extra. Its
  task is `django_domain_events.deliver_delivery`; a worker must list the module
  in Celery's `imports`.
- `@event(retention=...)`: a per-event lifetime for the event row and its
  deliveries. A `timedelta` keeps the event that long; `Retention.SUCCEEDED`
  deletes it once every delivery has succeeded, and `Retention.SETTLED` once
  every delivery is terminal, dead letters included. The policy is copied onto
  the event row when it is fired, so changing a declaration never rewrites the
  history already recorded. Firing an event declared with `Retention.SUCCEEDED` or
  `Retention.SETTLED` with a `dedupe_key` raises `ValueError`, because the key
  protects only while the row exists.
- `prune_events` honours those policies, sizes its batches in rows rather than
  events (`PRUNE_BATCH_ROWS`, default 5000; `--batch-size` on the command), takes
  a `stop` callable checked between batches, and records each receiver's last
  success in a small table so `quiet_receivers` outlives the deliveries it was
  read from.
- `RELAY_PRUNE_SECONDS` (default 60): how often an idle relay sweeps; see
  `RELAY_PRUNE` under Changed.
- `WAKE` (`"notify"` or `"poll"`, default `"notify"`) and
  `NOTIFY_COALESCE_SECONDS` (default 0.5): the Postgres wake-up is coalesced per
  process, so a burst of events sends one notification rather than one each.
- Receivers take `backoff_base_seconds=`, `backoff_cap_seconds=` and `lane=`.
  `run_relay(lane=...)` and `deliver_pending(lane=...)` serve one lane, and both
  take `batch_size=`, which sizes their claims alone. The default lane serves
  every receiver that names none.
- `DeliveryContext.delivery_id`, and the catalogue shows a receiver's own retry
  curve, lane and `give_up_after`.
- `RetryAfter(seconds, counts=False)`: a deferral that does not spend an attempt,
  for a destination limiting how much it accepts. It is bounded by
  `@receiver(give_up_after=timedelta(...))`, measured from when the row became
  owed, past which the next deferral dead-letters it. The relay pauses the
  receiver's lane for the requested time and hands back the rest of the batch
  rather than spending a call on each row; `counts=False` needs a wait above zero
  and falls back to a counting retry, with a warning once, when no
  `give_up_after` is declared.
- System checks `E006` to `E010` for the new settings, and `W001`/`W002` now read
  the owed indexes instead of scanning the delivery table.
- `deliver_events --lane` and `--batch-size`, and `CatalogueEvent.retention_seconds`
  and `delete_when` in the catalogue.
- The admin shows a delivery's target digest as hex.
- The example shop demonstrates per-receiver backoff, a mail lane, retention and
  a non-counting deferral.

### Changed
- **`RELAY_PRUNE` defaults to `True`.** A relay that finds nothing to claim now
  runs `prune_events()` itself, at most every `RELAY_PRUNE_SECONDS`, so a
  project that never scheduled the prune stops growing without a cron entry. A
  project that never pruned and holds more than `RETENTION_DAYS` of settled
  events will start deleting them within a minute of a relay going idle, and the
  first sweep deletes the whole backlog before the relay claims again, a batch
  at a time. Set `RELAY_PRUNE = False`, or run `prune_events` by hand, before
  upgrading if that is not what you want. A failing sweep is logged and the
  relay goes on; a stop request waits for at most one batch.
- **Migration `0006` rewrites tables.** On Postgres it converts `target_digest`
  to `bytea` in place (a table rewrite under an exclusive lock), drops and
  re-adds the event foreign key (re-validating every row) and builds five
  indexes, one in `0007`, without `CONCURRENTLY`. Run it in a maintenance window
  on a large delivery table; `sqlmigrate` prints the statements without
  converting anything.
- **Task messages queued by 0.9 fail in a 0.10 worker.** They carry only the
  delivery id, and the task now takes the claim as well; the rows recover once
  their lease lapses. Drain the queue before upgrading workers.
- **`deliver_one` refuses rows that are not owed** to any caller: a settled or
  dead row, or one still backing off, returns `None` instead of running the
  receiver. It used to force a run.
- **A task backend's `enqueue` takes the claim.** `TaskBackend.enqueue(delivery_id,
  claimed_by, claimed_at)` carries the relay's claim token on the message, and
  `deliver_one` takes the row with one conditional `UPDATE` on that token before
  the receiver runs. A custom backend must forward the two new arguments to
  `deliver_one`. `DjangoTasksBackend` does so.
- The target digest is stored as raw bytes (`BinaryField`) rather than a 64
  character string, converted in place on Postgres by migration `0006`. The
  migration also adds `due_at`, the retention columns and the partial indexes the
  scraped queries and the prune use; `0007` adds the last-success table and an
  index over unfinished rows.
- `outbox_health()` and `quiet_receivers()` are answered from those indexes and
  no longer scan the delivery table.
- `deliver_events` stops gracefully on `SIGTERM` or `SIGINT`: it finishes the
  delivery under way, hands the rest of its batch back and exits, and a second
  signal exits at once.
- `replay_events` and `requeue_dead` reset `due_at`, so a replayed row is
  measured from when it was replayed.

### Fixed
- A stale or redelivered task message ran the receiver on a row it no longer
  owned; the take now refuses any row that is not CLAIMED by the same worker at
  the same claim time.
- The relay exited when its claim raised, for instance during a database
  restart. It now backs off, capped at fifteen seconds, and carries on.
- A prune could delete deliveries a concurrent `replay_events` or `requeue_dead`
  had just reopened, after reporting them reopened: the prune re-checked that
  the event was settled against a snapshot taken before the replay committed.
  The event row is now locked, in primary key order, before the re-check, by
  the prune and by both operations. This predates 0.10.0 on the whole-event
  path; the per-event chunks and the relay's own sweep make it likelier.
- The example shop overstated its retries.

## [0.9.0] — 2026-09-16

### Added
- `PermanentFailure`: a durable receiver raising it dead-letters its delivery on
  the attempt that raised it, rather than spending the rest of the attempt budget
  on a destination that has already said it will never accept another request.
  The row is `DEAD` with that attempt counted, and `on_failure` is called with
  `DEAD`, exactly as when the budget runs out, so a failure log needs no second
  case to recognise it.
- `RetryAfter(seconds=..., reason=...)`: a durable receiver raising it schedules
  its next attempt that many seconds from now instead of on the backoff curve.
  It **consumes an attempt**, so a destination answering `429` forever still
  dead-letters within the declared budget. A negative or NaN delay is refused
  where it is constructed.
- `MAX_RECEIVER_RETRY_DELAY_SECONDS`, default one day: the ceiling on a
  `RetryAfter`. A longer request is clamped, with a warning naming the receiver,
  the delivery and both numbers. It is not `BACKOFF_CAP_SECONDS`, which bounds the
  curve; clamping a destination's hour-long request to that ceiling would retry
  something that asked to be left alone.
- The example shop demonstrates both: a marketplace answering `410 Gone`
  dead-letters on its first attempt, a carrier's `Retry-After: 120` schedules the
  next attempt two minutes out, and a two-day request is clamped to the ceiling
  with its warning. Each is checked, so the demo fails if one stops being true.
- The delivery docs cover `on_failure` and `DeliveryFailure`, which 0.8.0 shipped
  with only a docstring.
- `AnyEvent`: a receiver declared for it is owed every event fired. The wildcard
  is matched when an event is fired rather than expanded when the receiver is
  declared, so it reaches events declared by apps that load after it - the case
  a registry walk at startup silently misses. It is listed once, in a section of
  its own, by the catalogue; `what_listens_to(SomeEvent)` leaves it out and
  `what_listens_to(AnyEvent)` returns exactly the wildcards.
- `receiver(..., targets=...)`: a callable taking the event and a
  `DeliveryContext` and returning strings. `fire()` writes one delivery row per
  target instead of one per receiver, each with its own attempts, backoff and
  dead-letter, and writes none when it returns nothing. A target returned twice
  is delivered once; a non-string or blank target is refused.
  **The callable runs at fire time inside the caller's transaction**: if it
  raises, `fire()` raises and the caller's change rolls back with the event, and
  it costs its query on every event it is owed.
- `DeliveryRecord.target`, `DeliveryContext.target` and `DeliveryFailure.target`
  carry which target a delivery is for, blank for a receiver without `targets=`.
  The column is text, and nothing in the package limits its length.
  The catalogue publishes where a fan-out receiver's targets come from.
- The example shop forwards every event to the partners a table says want it,
  through one `AnyEvent` receiver with `targets=`, and checks that each partner
  gets a row, that an event nobody wants gets none, and that a replay after the
  subscriptions change reopens, adds and leaves alone exactly what it should.

### Changed
- `replay_events` calls a fan-out receiver's `targets` again, so a replay goes to
  the targets that exist at replay time. A target no longer returned is left as
  it was and not counted.
- The delivery table's unique constraint is now
  `(event, receiver_key, target_digest)`, replacing `(event, receiver_key)`.
  `target_digest` is the SHA-256 of the target, derived whenever a row is
  written, so the index entry is 64 characters however long the target is and
  the target text itself sits in no index. **Migration `0005` adds both columns
  and rebuilds that index**: every existing row gets the blank target and its
  digest, which is what a receiver without `targets=` writes, so nothing is
  backfilled - but building a unique index takes a lock proportional to the
  table on some backends, so prune before migrating a large one.
- `check_receivers_have_events` (`E001`) no longer reports a wildcard receiver,
  whose `AnyEvent` is a marker rather than a declared event.
- The delivery admin shows and searches by target.
- `tests/` mirrors the package again. The layout regroup in 0.8.0 moved the source
  into subpackages and rewrote the test imports without moving the test files, so
  the two stopped matching what `CLAUDE.md` requires. Twenty-nine files moved; six
  stay at the test root because their subjects do.

## [0.8.0] — 2026-09-11

### Added
- `receiver(..., on_failure=...)` and `DeliveryFailure`. A receiver can now keep a
  durable record of its own failure, which it could not before: it runs inside the
  transaction carrying its acknowledgement, so everything it writes is discarded
  the moment it raises, and the attempts worth logging were exactly the ones that
  could not be logged. The hook runs after the delivery row is written and outside
  that rolled-back transaction, so what it writes survives.
  It is called for `FAILED` and for `DEAD`, because "it failed again" and "it will
  not be tried again" are different things to record. It is not called by a worker
  whose write landed nowhere, since whoever legitimately holds the row will report
  its own outcome. A hook that raises is logged and swallowed: a failure path that
  fails is worse than a lost log line.

### Changed
- Layout: the package root now holds `__init__.py`, `version.py`, `settings.py`,
  `apps.py`, `checks.py`, `utils.py` and `payload_upgrade_failed.py`. Everything
  else moved into a subpackage named for a concern: `declaration/`, `delivery/`,
  `scope/`, `introspection/`, `operations/` and `testing/`. **The public surface
  is unchanged** - all fifty symbols are still importable from the package root,
  which is where they were always meant to be imported from. Only an import of an
  internal leaf path is affected.
- `DeliveryRecord.__str__` names the event rather than its id:
  `receiver <- shop.OrderPlaced#42` where it previously read
  `receiver <- event 42`. `EventRecord.__str__` is already `name#pk`, so this
  gains the event name and keeps the join key. It traverses the relation, which
  costs one query on an instance that did not fetch it; the string renders on the
  delete confirmation page, in object history and in related-field widgets, not
  in the changelist.

### Removed
- The `event_id: int` annotation that existed only so `ty` could see an attribute
  Django creates at runtime. Traversing the relation removes the need for it.

## [0.7.0] — 2026-09-05

### Added

- **Both codecs decode `tuple` fields.** An event is a frozen dataclass, so a
  `tuple` is the sequence type that belongs in one -- a `list` field is a
  mutable field in a nominally immutable object. The package therefore steers
  every consumer towards `tuple[X, ...]`, and until now neither codec could
  read one back: `DataclassCodec` refused it and pointed at `DaciteCodec`, and
  `DaciteCodec` refused it too with a `WrongTypeError`, because JSON has no
  tuple and the encoder had written a list. `list[X]` worked in both, so the
  hint sent readers to a codec that failed the same way for a different reason.

  Fixed-length tuples decode as well, and a payload of the wrong length is
  refused rather than truncated or padded.

- **A system check that declared events can actually be decoded** (`E005`).
  The existing codec check asks whether the codec can be *imported*; this one
  asks whether it can rebuild the events this project declares, which is the
  question whose answer is silent when it is no. `fire()` encodes and commits
  whatever the annotation says, so an undecodable event is recorded
  successfully inside the caller's transaction and then dead-letters on every
  durable delivery -- in the relay, in another process, possibly hours later.

  Codecs answer through a new optional `unsupported_fields()`. A codec that
  does not implement it is not interrogated, because this package should not
  guess at what a codec it did not write can do.

- **Two system checks for the settings block** (`W006`, `W007`), because both
  mistakes are otherwise silent. `setting()` reads only the keys this package
  asks for, so an unrecognised key sits in the settings looking effective; and
  the dict is `DJANGO_DOMAIN_EVENTS` while the app label, the import path and
  most prose say "domain events", so a `DOMAIN_EVENTS` block returns nothing
  and every value falls back to its default with nothing said.

  That second case is why this shipped alongside `E005` rather than after it: a
  consumer set `CODEC` correctly in a misnamed block, saw no effect, and spent
  a round debugging a decode failure with the fix visibly in place.

- **`DeliveryContext.actor_label`**, beside `actor_key`. `attributed()` already
  accepted the label and `EventRecord` already persisted it; the object handed
  to a receiver that declared `takes_context=True` was the one place it did not
  reach. `actor_key` is `auth.User:1`, which is right for joining and wrong for
  the line an audit reader sees, and that difference is why the two are separate
  fields on the row.

  Both construction sites read it off the event row rather than the ambient
  scope, so a durable delivery running hours later in another process gets the
  same value as an inline one.

### Notes
- **The `ty` floor was raised to `0.0.32`, because the declared one was false.**
  `ty==0.0.1a10` cannot parse the `[tool.ty.environment]` table this repository
  has shipped since its first commit -- it fails with a TOML parse error. The
  `lowest declared versions` job passed only because the resolver it runs under
  rounds the pre-release up. A floor nothing can resolve to is not a floor.

## [0.6.0] — 2026-09-01

### Added
- `lease_seconds=` on a receiver, overriding `LEASE_SECONDS` for one that runs
  long. A receiver cannot extend its own lease and no API offers to: it runs
  inside the transaction carrying its acknowledgement, so a lease extension it
  writes is invisible to every other worker until it has already finished.
  Measured on Postgres - while the receiver had pushed its lease an hour out,
  another connection still read the original expiry.
- `upgrade(payload, from_version)` on an event class, the escape from the two
  payload changes the schema rule calls breaking. It must be a `staticmethod` or
  `classmethod`, checked at the decorator, because an ordinary method reached
  through the class is unbound and the payload would silently arrive as `self`.
  It runs only when the row is older than the declaration, and on every decode
  path, so `assert_fired` cannot read a payload the relay would reject.
- `PayloadUpgradeFailed`, so a failing hook is named in `last_error` rather than
  something unspecified going wrong between the row and the receiver.
- `outbox_health()` and `manage.py events_status`. `quiet_receivers()` answers
  whether a receiver is running; nothing answered whether the queue is draining,
  and those fail differently. `oldest_owed_at` is measured from when the work
  arrived rather than when it next becomes due, because the backoff pushes a
  failed delivery's `available_at` into the future and an age read off that
  column goes negative exactly while a receiver is failing. Dead letters are
  counted but deliberately not owed, or a backlog alert fires forever after one
  bad deploy - so alert on `dead` as well.
- The relay logs at `WARNING` when a worker loses a delivery, naming the
  receiver whose `lease_seconds=` to raise.
- `examples/shop`, a runnable project using every part of the package for
  something an application would actually want, with a `demo` command that
  prints what the log recorded at each step. A CI job runs it, because an
  example nothing executes drifts from the API it teaches and the drift is
  silent.

### Changed
- `dispatch_one` extends the lease before handing a row to a task backend. It
  did not, so `lease_seconds=` was silently ignored on the one path where the
  wait is the point: a queue backlog longer than `LEASE_SECONDS` had the relay
  reclaim the row while the task worker still held it.
- The `upgrade` hook's result is decoded under the **declared** version, not the
  row's. A codec is handed the version so it can branch on it, and telling it
  the old one asks for the old treatment of newly-shaped data.
- A hook that returns something other than a mapping raises
  `PayloadUpgradeFailed` naming it. Mutating the payload in place and forgetting
  to return is the commonest way to write it wrong, and it dead-lettered with
  "argument of type 'NoneType' is not iterable".
- `lease_seconds` must be positive. Zero expires the instant before the receiver
  starts, so a second relay reclaims the row immediately and both run it.
- `events_status` reports each receiver's own oldest-owed age, in both formats.
  It was documented as the number an alert is written against and reachable in
  neither.
- The catalogue publishes `lease_seconds` and whether an event declares
  `upgrade()`. Found by generating it for a real app: it had drifted from the
  declarations, which is the one thing it claims cannot happen.
- The Markdown catalogue blanks `site`, `max_attempts`, `eager` and the lease
  for non-`DURABLE` receivers. They are defaults nobody chose, and `5` beside an
  `INLINE` receiver reads as a retry budget it will never have.
- `max_attempts=`, `eager=` and `lease_seconds=` are refused on a non-`DURABLE`
  receiver, the way `site="task"` already was. They describe a delivery row that
  mode never creates, so the declaration stated something nothing would honour.

## [0.5.1] — 2026-09-01

### Fixed
- The `succeeded_at` column added in 0.5.0 arrived empty, so on upgrading every
  receiver read as having never succeeded - which is the one question
  `quiet_receivers()` exists to answer, wrong on day one for anyone with
  history. A data migration backfills it from `completed_at` for deliveries
  still `succeeded`, where that column is the moment the delivery was
  acknowledged. A dead letter settled too and is not backfilled; a replayed row
  had `completed_at` cleared by the replay, so nothing survives to recover and
  inventing a timestamp would be worse than the null.

## [0.5.0] — 2026-08-31

### Added
- `catalogue()` and `render_catalogue()` - every declared event, its payload
  schema and its receivers, generated from the declarations so it cannot drift
  from them. Markdown for a person onboarding, JSON for a pipeline that fails a
  pull request when a field other teams consume disappears. Sorted throughout,
  ending in exactly one newline, and escaping pipes inside table cells, so a
  checked-in catalogue diffs cleanly and `str | None` does not silently shift
  every later column of its row.
- `manage.py export_catalogue [--format markdown|json] [--output PATH]`.
- `what_listens_to(EventClass)` - every receiver, across all modes, sorted by
  key. A Django signal's receivers are weak references behind an opaque dispatch
  uid, so "who reacts to this" is otherwise answerable only by grepping.
- `listens_for(receiver_key)` - the inverse, and the direction an operator needs:
  a dead-letter row names a key, and the next question is what it was owed.
- `quiet_receivers()` and `manage.py quiet_receivers [--days N]` - durable
  receivers that have succeeded at nothing inside the window. Driven by the
  registry rather than the table, so a receiver that has **never** received
  anything appears; a query over delivery rows alone cannot produce that answer,
  because there is no row to find. The window defaults to `RETENTION_DAYS`,
  which is the longest honest answer: past it the prune deleted the evidence.
- `W002`, a system check for **deliveries owed under an event name the registry
  no longer has**. This is the renamed event, and `W001` structurally cannot see
  it: the receivers keep their keys, so nothing looks orphaned, while every row
  written under the old name decodes to nothing and spends one attempt budget at
  a time finding out. Limited to work still owed, because warning about settled
  history on every `check` run teaches the reader to skip the output.
- A read-only admin for both models, registered by Django's own autodiscovery,
  with **Replay selected events** and **Requeue selected dead deliveries**.
  Read-only on purpose: the one guarantee this package sells is that a row exists
  if and only if the change committed, and a form that can write one is a way to
  break it. Deleting is refused too - it would cascade owed deliveries away with
  no record that anything was lost, which is what `prune_events` re-checks for.
  Its filters come from the registry rather than `SELECT DISTINCT` over the
  log, so opening the changelist scans nothing and a declared-but-never-fired
  event is still listed.
- `requeue_dead(delivery_ids=...)` - scope a requeue to named rows, for an
  operator reading a dead-letter list and picking the ones they understand. An
  empty list requeues nothing, and the id list narrows the selection rather than
  widening it past `DEAD`. The selection is chunked like the update it feeds,
  so an admin select-all over a dead-letter queue past SQLite's
  32,766-parameter ceiling does not turn a routine requeue into an error.
- A documentation site with pages for declaring, delivery, scope, operations,
  introspection, settings and the API reference.
- `DeliveryRecord.succeeded_at` - when a delivery last succeeded, never cleared.
  Separate from `completed_at`, which replay and requeue clear because a
  reopened row has not settled again yet. Reading the quiet-receiver query off
  the cleared column meant an operator who replayed yesterday's events to re-run
  a receiver they had just fixed was then told it had never run at all.

### Changed
- `check_no_orphaned_deliveries` (`W001`) now treats **any non-terminal**
  delivery as owed, where it listed `pending` and `failed` and so missed
  `claimed`. A worker that dies between claiming a row and the deploy that
  deletes its receiver leaves the row claimed under a lapsed lease; that read as
  settled until a relay happened to reclaim it. Both checks and the prune now
  share one definition of owed, which is also the one the relay claims by.

### Security
- Both admin actions now require the model's **change** permission. Django
  offers an action with no declared permission to anyone who can reach the
  changelist, and `has_change_permission` gates the change *form* alone - so
  view-only staff could replay the entire log, re-running every durable
  receiver, and empty the dead-letter queue. The edit form stays refused either
  way.

## [0.4.0] — 2026-08-31

### Added
- `prune_events()` and `manage.py prune_events` - retention. An outbox without
  a prune story becomes the largest table in the database, and it becomes it
  quietly. Only settled events go: one with a delivery still pending, failed or
  claimed is still owed, and deleting it would drop work nobody recorded as
  lost. Deletes in batches, because a single statement over a year of rows holds
  a lock for as long as it runs on the table the relay claims from.
- `replay_events()` and `manage.py replay_events` - make events owed again. The
  receiver set freezes at fire time so a deploy never hands a new receiver a
  backlog; this is the other half of that, and it counts reopening a terminal
  delivery separately from adding one for a receiver that did not exist.
- `requeue_dead()` and `manage.py requeue_dead` - give dead-lettered deliveries
  their attempt budget back, scoped to one receiver when the reason is that one
  downstream was broken.
- `LISTEN`/`NOTIFY` on Postgres. The relay waits on a notification instead of
  sleeping, so an event fired a moment ago is delivered in milliseconds. The
  poll stays as the floor: a notification sent while nobody is listening is
  lost, so this removes latency and never carries the obligation.
- The `task` execution site. `@receiver(..., site="task")` hands the delivery to
  the configured `TASK_BACKEND` instead of running it in the relay; the task
  acknowledges the row when it finishes. Django Tasks is the first adapter, and
  it works through `django.tasks` on 6.0+ and the `django-tasks` backport below
  that.
- Settings: `RETENTION_DAYS` (90) and `TASK_BACKEND` (none).

### Fixed
- `replay_events()` and `requeue_dead()` could clear a live lease. Both read the
  status in one statement and wrote in another with no predicate, so a delivery
  claimed in between had its lease wiped while its worker was mid-receiver -
  two workers on one delivery, which is the one thing the lease exists to
  prevent. Both updates now carry a status predicate.
- `prune_events()` could delete work a replay had just created, between its
  select and its delete, cascading away rows the operator had been told were
  reopened. Settledness is re-checked at the delete.
- `requeue_dead(limit=0)` requeued everything. An operator asking for the
  smallest possible blast radius got the largest one.
- `requeue_dead()` built one statement over every dead row: past 32,766 it fails
  on SQLite, and on Postgres it took every row lock at once. It chunks now, and
  clears `last_error` so a requeued row that later succeeds does not still show
  why it died.
- The execution site was honoured only by the relay. `deliver_events --once`,
  `drain_outbox()` and the eager path all ran a `site="task"` receiver in
  process - `drain_outbox()` most sharply, since it promises to run the same
  path as production.
- `site="task"` with no `TASK_BACKEND` ran silently in the relay; it now refuses.
  `site="task"` on an INLINE or ON_COMMIT receiver is refused at declaration,
  because those have no delivery row to hand over.
- The task backend is built only for a receiver that asked for one, so a
  misconfigured `TASK_BACKEND` no longer breaks receivers that never wanted it.
  It also accepts a mapping with `BACKEND` plus options, which a dotted path
  alone made unreachable.
- A database failure during the relay's idle wait no longer kills the daemon.
  The wait reaches past Django's cursor to the driver, so the exception is not a
  `django.db.Error` and a supervisor written to catch that would miss it.
- `replay_events()` uses one transaction per event, so a unique-constraint
  collision on one no longer discards the reopens for every other event in the
  call, and tolerates a concurrent replay creating the same row.
- `replay_events()` and `requeue_dead()` wake a waiting relay, as `fire()` does.

### Notes
- An enqueued delivery stays claimed under its lease and is counted as no
  outcome, because nothing has happened to it yet. If the enqueue is lost the
  lease lapses and the relay reclaims it - which is what makes handing work to a
  lossy queue safe, and why the adapter protocol is a single method.

## [0.3.0] — 2026-08-31

### Added
- `attributed()` - attach an actor and arbitrary facts to every event fired
  inside a block. Nested blocks layer rather than replace, so a request-level
  actor survives an inner block that only adds a source. The actor's identity is
  derived once, at capture, into the three columns the row already carried.
- `suppressed()` - fire without delivering, and say why. The reason is required
  and lands on the row, because a silently dropped event is unauditable, which
  is the failure mode suppression is most likely to cause. `record=False`
  discards instead, for the bulk import where a row per suppressed event is the
  surprise.
- Correlation and causation. The outermost `attributed()` block roots a chain;
  an event fired from inside a receiver records its parent automatically, and
  inherits the chain from the row it descended from - so a grandchild delivered
  hours later in another process still belongs to the request that started it.
- `propagate_scope()` - carry the scope into a thread you start yourself.
  `threading.Thread` and `ThreadPoolExecutor.submit` begin with an empty context
  rather than a copy, so a worker silently loses the attribution of whoever
  spawned it. Nothing fails; events simply arrive with no actor.
- `Scope` and `current_scope()` for reading what is in effect.

### Changed
- `fire()` now returns `int | None` rather than `int`. It returns `None` when a
  `suppressed(..., record=False)` block discarded the event without recording
  it, and a caller annotating the result as `int` will stop type-checking.

### Notes
- Scope is captured at fire time, in the firing process, and every downstream
  reader takes attribution off the row. That is a rule rather than an
  implementation detail: `on_commit` callbacks run at commit, which can be after
  the `with attributed(...)` block has exited, and a durable delivery can run in
  another process hours later. Three tests hold it, and reading the scope lazily
  instead fails eight.

## [0.2.0] — 2026-08-31

### Added
- The relay. `deliver_events` without `--once` claims and delivers continuously;
  `run_relay()` is the same loop as a function.
- Leased claims over `SELECT ... FOR UPDATE SKIP LOCKED`. A claim carries an
  expiry, so a worker that dies without acknowledging becomes re-claimable when
  its lease lapses - the same path as an ordinary retry rather than a special
  case. Two workers never take the same row.
- Exponential backoff with full jitter, expressed as `available_at`, so the
  claim query is what honours a wait and nothing else has to remember it.
- `eager=True` on a receiver: attempt delivery immediately after commit in the
  firing process, with the relay as the fallback. Outbox durability at
  on-commit latency, at the cost of a duplicate when the process dies
  mid-receiver.
- `claim_batch()` and `backoff()` are public, so a consumer building its own
  worker does not have to reimplement the claim protocol.

### Changed
- `deliver_events --once` and `drain_outbox()` now claim through the same leased
  path, so a one-shot pass and a running relay cannot hand the same row to two
  receivers.
- Every acknowledgement is a compare-and-set against the claim the call started
  with, and the lease is extended to cover the one delivery about to run. A
  batch claim otherwise stamps a single expiry across every row it took while
  the relay delivers them serially, so the lease ran out partway through the
  batch and a second relay reclaimed rows the first was still inside.
- `deliver_one()` takes a `worker_id` and returns `None` when the row is no
  longer that worker's. Losing a claim is an ordinary outcome of a lease
  expiring, not a fault.
- `claim_batch()` locks as strongly as the backend allows: skipping where
  supported, blocking `FOR UPDATE` where not, and unlocked only where there is
  no row locking at all.
- `run_relay()` survives an unexpected failure on one row rather than dying
  mid-batch and stranding everything it had claimed.
- `django.contrib.auth` is documented as required. The event row's `actor`
  foreign key targets `AUTH_USER_MODEL` and the migration declares a swappable
  dependency on it, so a project without it could not migrate - and nothing
  said so.
- `suppressed_reason` says it is reserved rather than describing behaviour that
  does not exist yet.
- The relay refuses to start where the backend cannot skip locks.
  `allow_unsafe_concurrency=True` lifts that for a deployment running exactly
  one relay.

### Fixed
- **The claim query could never use its index**, so every relay pass scanned the
  whole delivered history. The partial index was conditioned on
  `status = pending` while the query reads `status IN (pending, failed)`, and a
  predicate on one cannot serve the other. Measured on Postgres 16 with 500k
  delivered rows and 1,000 owed: a parallel sequential scan discarding 166,667
  rows per worker, now an index scan. There is a second partial index for the
  lapsed-lease arm, and the redundant full index on `available_at` is gone.
- **Stacking `@receiver` on one function silently discarded all but one
  subscription.** The collision guard tolerated a repeat registration of the
  same callable, to survive a double import - but that is exactly the shape of
  a stacked decorator, so the second registration replaced the first and the
  event fired to nothing. The guard now compares the whole registration, which
  also catches a re-registration that changes `mode` or `max_attempts`.
- **A plain `Enum` could not be encoded**, though the decoder advertised enums:
  `DjangoJSONEncoder` has no `Enum` branch, so `fire()` raised a bare
  `TypeError` from inside the caller's transaction. Only a `str`-mixin enum
  survived, which is the kind the tests used.
- **`datetime` and `time` were truncated to milliseconds**, so an event came
  back from the log with different data and nothing failed - `assert_fired()`
  least of all, which is the helper most likely to be pointed at exactly that.
- **A value that cannot be written now names its field** and raises
  `UnsupportedPayloadType` rather than a raw error from `json` naming only a
  type.
- **Every transaction and connection now follows the database router.**
  `deliver_one()` opened `atomic()` on `default` and `fire()` asked `default`
  whether a transaction was open, so an event log routed to its own database
  lost the guarantee that a receiver's work and its acknowledgement commit
  together, and `WARN_OUTSIDE_ATOMIC` warned the caller who had it right while
  staying silent for the one who did not.
- **`assert_fired()` no longer uses bare `assert`**, which `python -O` strips -
  a published assertion helper that passes on any input under optimisation.
- **0.1.0 could not be installed.** `manage.py migrate` failed on a fresh
  database with `no such table: django_domain_events_deliveryrecord`, creating
  no tables at all, on every supported Django version. The orphaned-delivery
  system check queried its table unconditionally: it needs a `databases` guard,
  because `check`, `makemigrations` and `showmigrations` pass none, and a
  table-existence guard, because `migrate` does pass one and runs the check
  before creating the tables.

## [0.1.0] — 2026-08-31

### Added
- The contract, whole. `fire()` records an event row and one delivery row per
  durable receiver inside the caller's transaction, so the obligation exists if
  and only if the business change committed.
- `@event` and `@receiver` declarations, with a registry that can answer what
  listens to what. Names default to `<app_label>.<name>` rather than the dotted
  import path, so moving a module does not orphan pending rows.
- `INLINE` and `ON_COMMIT` delivery, alongside `DURABLE`. `ON_COMMIT` receivers
  are registered with `robust=True`, so one failing does not cancel the others
  registered in the same transaction.
- `takes_context=True`, following `django.tasks.task`, with overloads that make
  a type checker enforce the arity the flag implies.
- Both database tables complete, including the attribution, correlation and
  causation columns the later milestones fill in. A durable log's migrations are
  the expensive thing to change once a consumer has rows in it.
- A payload codec seam over plain frozen dataclasses. `DataclassCodec` (the
  default, no dependency) handles flat payloads of documented scalars and
  refuses everything else by name; `DaciteCodec`, behind the `dacite` extra,
  handles nested shapes. Both parse datetimes the way `DjangoJSONEncoder` writes
  them, which differs from what `datetime.fromisoformat` accepts before Python
  3.11.
- `deliver_events --once`, `drain_outbox()` and `assert_fired()`. The test
  helper runs the real delivery path rather than bypassing it, and
  `assert_fired()` returns the events decoded from the log, so a payload that
  cannot round-trip fails the assertion too.
- System checks for a receiver listening to an undeclared event, an unimportable
  codec, and delivery rows addressed to a receiver that no longer exists.


### Notes
- A single delivery pass, with no leased claim and no `SELECT ... FOR UPDATE
  SKIP LOCKED`. Two passes running at once will both claim the same rows and
  deliver twice; at-least-once already requires receivers to tolerate that, but
  the concurrent relay is not here yet, which is why `--once` is required rather
  than defaulted.

[Unreleased]: https://github.com/Artui/django-domain-events/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/Artui/django-domain-events/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/Artui/django-domain-events/compare/v0.11.2...v0.12.0
[0.11.2]: https://github.com/Artui/django-domain-events/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/Artui/django-domain-events/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/Artui/django-domain-events/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/Artui/django-domain-events/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/Artui/django-domain-events/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/Artui/django-domain-events/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/Artui/django-domain-events/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/Artui/django-domain-events/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/Artui/django-domain-events/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/Artui/django-domain-events/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/Artui/django-domain-events/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Artui/django-domain-events/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Artui/django-domain-events/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Artui/django-domain-events/compare/v0.0.0...v0.1.0
