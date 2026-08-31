# CLAUDE.md

Guidance for working in this repository.

## What this package is

A Django domain-event log with in-process fan-out. `fire()` records a typed
event to a database table **inside the caller's transaction**; a relay delivers
it to registered receivers afterwards, at-least-once, with per-receiver retry
and dead-lettering.

The one sentence everything else follows from: **the event exists if and only if
the change committed.** That is what closes the dual-write gap, and it is why
`fire()` means "record intent" rather than "call receivers".

It is deliberately **not** a signals replacement, and must not be positioned as
one: a database write per event rules out chatty notification use. What it is
instead is a log with three properties signals structurally cannot have - a
crash story, durable attribution, and queryable introspection.

No dependency on the AG-UI / drf-services family, in either direction, now or
planned.

## Commands

| Command | Does |
| --- | --- |
| `make init` | Sync all dependency groups and install the pre-commit hooks |
| `make test` | pytest with the 100% line+branch coverage gate |
| `make lint` | `ruff check` plus `ty check` |
| `make format-check` | `ruff format --check --diff` (CI runs this; `make lint` does not) |
| `make docs-build` | `mkdocs build --strict` |
| `make release-bump VERSION=X.Y.Z` | Rewrite the version and promote the changelog section |

Run the suite against Postgres locally with `DDE_TEST_DATABASE=postgres` and the
usual `PG*` environment variables pointing at a server.

## Structural rules

Non-negotiable. They keep the package navigable.

1. **One exported class or function per file.** The file is named after the
   exported symbol in `snake_case`.
2. **Private helpers used only in one file** stay in that file with a `_name`
   prefix.
3. **Non-exported helpers shared across files** go in that package's `utils.py`.
4. **Top-level imports only.** Lazy or function-local imports are forbidden
   unless a circular import is proven, or the import targets a declared optional
   dependency gated behind an opt-in. Document the reason inline.
5. **Full type annotations on every function and method signature.** `Any` is
   allowed only at the Django boundary where the type genuinely is `Any`.
6. **`__init__.py` is the only re-export point.** Each `__init__.py` lists the
   public surface in `__all__`. Internal modules import from leaf paths, never
   from the package's `__init__`.
7. **Types live in `types/`.** Value-shape carriers live under `types/`;
   behavioural code lives at the package root.

## Three constraints that look like tidy-ups

Each of these reads as an oversight and is not. They are here rather than as
comments in the files because the code cannot carry the reason at the point
someone would change it.

- **The re-exports in `__init__.py` are eager, and must stay eager.** A lazy PEP
  562 `__getattr__` is the obvious alternative and it does not hold: under
  one-symbol-per-file the module and the symbol share a name, so importing
  `django_domain_events.fire` binds the *module* as an attribute of the package
  and `from django_domain_events import fire` then returns a module. Caching the
  resolved value only wins if nothing imports the submodule afterwards, and the
  internals do.
- **The four model-touching modules import models inside their functions.**
  Django imports an app's package before the app registry is ready, and
  `__init__` re-exports those functions, so a module-level model import raises
  `AppRegistryNotReady` at startup. Nothing outside `models/` names a model in a
  signature, so the annotations need no import either.
- **`codecs/__init__.py` does not export `DaciteCodec`.** It imports an optional
  extra; re-exporting it would pull `dacite` in for anyone touching the package
  at all. It is referenced by full dotted path in settings.

## Adding a feature

Branch first, always. Three touchpoints per change: the source file, the
`__init__.py` re-export, and the mirrored test file.

## Tests

`tests/` mirrors the source tree, one file per source file with the same name.
`pytest-asyncio` runs in auto mode. The coverage gate is **100% line and
branch**.

Never `# pragma: no cover`. If a branch cannot be reached, that is a signal to
restructure the code, not to exempt it.

Two rules specific to this package:

- **Every guarantee needs a test that fails when the guarantee is removed.**
  Delete `robust=True` from the `on_commit` registration and a test must go red.
  Replace a leased claim with mark-and-hope and a test must go red. Read a
  `ContextVar` inside deferred code and a test must go red. A test that passes
  either way is documentation, not a gate.
- **Concurrency is tested with real connections.** A `SKIP LOCKED` test that
  runs on a single connection proves nothing. Those tests need two threads and
  `transaction=True`, and they only mean something on Postgres.

## Type checking

`ty`, scoped to `django_domain_events` via `[tool.ty.environment]`. The package
ships `py.typed`, so consumers get the annotations.

Never a mypy-style `# type: ignore` in the package - a pre-commit hook rejects
it, because nothing here reads that pragma and leaving one implies a checker
that is not running.

## Linting and formatting

`ruff` is the source of truth for both. Use `...` rather than `pass` for empty
bodies.

`make lint` does **not** run `ruff format --check`, and CI does. Run
`uv run ruff format --check` before pushing.

## Imports inside the package

Absolute and fully qualified, never relative - `ban-relative-imports = "all"` is
configured and enforced. isort order is stdlib, third party, first party.

## Compatibility floor

| | Minimum | Tested against |
| --- | --- | --- |
| Python | 3.10 | 3.10 through 3.14 |
| Django | 4.2 | 4.2, 5.0, 5.1, 5.2, 6.0, 6.1 |
| Postgres | 12 | 17 (the relay half only) |

The Django floor is **4.2 for a reason**: `transaction.on_commit(robust=True)`.
Without it, one best-effort receiver raising cancels every later callback
registered in the same transaction, which silently deletes the other receivers'
work. Do not lower it.

The relay requires `SELECT ... FOR UPDATE SKIP LOCKED`. Declaration, the
in-transaction and post-commit timings, and the test helper all work on SQLite;
the long-running relay refuses to start where the statement is unavailable, and
says why.

## CI and pre-commit

Eight jobs in `tests.yml`: `lint`, `docs`, `floor` (resolves every declared
dependency at the bottom of its window and runs the suite there), `test` (the
Python x Django matrix on SQLite), `test-postgres` (a real server, for the half
SQLite cannot exercise), `coverage-badge`, `secrets`, and the `tests-passed`
gate that branch protection points at.

Releases are **main-triggered**: `make release-bump`, edit the changelog, open a
PR, and merging to `main` runs the release. There is no tag to push - the
workflow creates the tag after PyPI accepts the upload, which is also why
`make release-publish-finalize` exists for the case where it does not.

Pre-commit runs gitleaks, the standard hygiene hooks, ruff, ty, and four
convention guards: no local filesystem paths, no internal plan-step labels, no
mypy-style type-ignore, and no emoji or marker glyphs in any committed file.

## Releasing

```
make release-bump VERSION=X.Y.Z   # rewrites version.py, promotes the changelog
# edit CHANGELOG.md to fill in the new section, review the diff
# open a PR, get it reviewed, merge to main
```

The release job on `main` short-circuits to a no-op when a `vX.Y.Z` tag for the
version in source already exists on origin, so an ordinary merge costs nothing.

That guard is why the scaffold sits at `0.0.0` with a matching `v0.0.0` tag. A
new repository has no tags at all, so without one the very first push to `main`
reads the scaffold version as unreleased and runs a real release attempt - which
gets as far as building distributions before failing on the missing changelog
section. Nothing is published and no tag is created, but the repository starts
life with a red release run. The first real release is
`make release-bump VERSION=0.1.0`.

One-time setup, none of which can be done from a checkout: a PyPI Trusted
Publisher pointing at this repo with workflow `release.yml` and environment
`pypi`; a `pypi` GitHub Environment; GitHub Pages serving from `gh-pages`; and
branch protection requiring the `tests-passed` check.
