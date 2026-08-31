# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- The relay refuses to start where the backend cannot skip locks.
  `allow_unsafe_concurrency=True` lifts that for a deployment running exactly
  one relay.

### Fixed
- **The package could not be installed.** `manage.py migrate` failed on a fresh
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

[Unreleased]: https://github.com/Artui/django-domain-events/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Artui/django-domain-events/compare/v0.0.0...v0.1.0
