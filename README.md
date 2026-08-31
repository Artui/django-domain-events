# django-domain-events

[![CI](https://github.com/Artui/django-domain-events/workflows/tests/badge.svg)](https://github.com/Artui/django-domain-events/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Python versions](https://img.shields.io/pypi/pyversions/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Django versions](https://img.shields.io/pypi/djversions/django-domain-events.svg)](https://pypi.org/project/django-domain-events/)
[![Docs](https://img.shields.io/badge/docs-artui.github.io-blue.svg)](https://artui.github.io/django-domain-events/)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Artui/django-domain-events/gh-pages/coverage.json)](https://github.com/Artui/django-domain-events/actions/workflows/tests.yml)

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

Nested event payloads need the decode half of the codec:

```bash
pip install "django-domain-events[dacite]"
```

## Status

Early development. The API is not stable and the package is not yet usable;
see the changelog for what has landed.
