# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
