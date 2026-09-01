# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/Artui/django-domain-events/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/Artui/django-domain-events/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/Artui/django-domain-events/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/Artui/django-domain-events/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Artui/django-domain-events/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Artui/django-domain-events/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Artui/django-domain-events/compare/v0.0.0...v0.1.0
