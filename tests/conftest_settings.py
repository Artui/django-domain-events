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
    "django.contrib.messages",
    "django.contrib.sessions",
    # Installed rather than stubbed: the admin integration is a package named
    # `admin`, and whether Django's autodiscovery imports it is exactly the
    # thing worth testing. A hand-built ModelAdmin in a test would pass with
    # the package never loaded.
    "django.contrib.admin",
    "django_domain_events",
    "tests.testapp",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

ROOT_URLCONF = "tests.urls"

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

# A second alias so the router tests have somewhere to route to. Nothing uses it
# unless a test installs DATABASE_ROUTERS: a package whose transactions name the
# default connection passes every single-database test and still breaks the
# moment an event log is given its own database.
DATABASES["events"] = dict(DATABASES["default"])
