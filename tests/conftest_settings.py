"""Minimal Django settings for the test suite.

The database is chosen by ``DDE_TEST_DATABASE`` so one suite can run against
both backends. SQLite is the default because that is what most Django projects
run their own tests on, and the declaration, timing and introspection halves of
this package work there. Postgres is selected in CI for the half that cannot be
tested anywhere else: ``SELECT ... FOR UPDATE SKIP LOCKED``, leased claims and
concurrent relays have neither the statement nor the concurrency model on SQLite,
so a green SQLite run is compatible with those claims being untested.
"""

import os

SECRET_KEY = "not-a-secret-this-is-the-test-suite"

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_domain_events",
]

USE_TZ = True

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

if os.environ.get("DDE_TEST_DATABASE") == "postgres":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("PGDATABASE", "django_domain_events"),
            "USER": os.environ.get("PGUSER", "postgres"),
            "PASSWORD": os.environ.get("PGPASSWORD", "postgres"),
            "HOST": os.environ.get("PGHOST", "localhost"),
            "PORT": os.environ.get("PGPORT", "5432"),
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    }
