"""Minimal Django settings for the test suite.

The database is chosen by ``DDE_TEST_DATABASE`` so one suite can run against
all three backends. SQLite is the default because that is what most Django
projects run their own tests on, and the declaration, timing and introspection
halves of this package work there. Postgres is selected in CI for the half that
cannot be tested anywhere else: ``SELECT ... FOR UPDATE SKIP LOCKED``, leased
claims and concurrent relays have neither the statement nor the concurrency
model on SQLite, so a green SQLite run is compatible with those claims being
untested.

``mysql`` runs the same suite on MySQL, which has the statement and the row
locks too and differs from both elsewhere: a multi-row insert returns no primary
keys, a DELETE may not read its own table in a subquery, and DDL is not
transactional, so a migration that fails part way stays part way. Each ``MYSQL_*``
variable names one connection setting; the defaults are a local server's root
user with no password. The user has to be able to create a database, because
Django creates ``test_<NAME>``. The driver, mysqlclient, is not in any
dependency group, since it builds against MySQL's client headers: install it
into the environment by hand.
"""

from __future__ import annotations

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

# ``DDE_TEST_USE_TZ=false`` runs the whole suite with naive datetimes, which a
# project still on ``USE_TZ = False`` stores and reads back in ``TIME_ZONE``
# local time. The package takes "now" from ``django.utils.timezone.now()`` so its
# values follow whichever convention the project chose; a CI job runs this
# setting on Postgres, because a comparison between an aware and a naive value
# raises ``TypeError`` and only a run under the other setting can find one.
USE_TZ = os.environ.get("DDE_TEST_USE_TZ", "true").lower() not in {"0", "false", "no"}

# Django's default, America/Chicago, on purpose: a zone other than UTC means a
# naive value read back under ``USE_TZ = False`` is visibly local time, so code
# attaching UTC to it is hours wrong and a test comparing it with
# ``timezone.now()`` says so. ``DDE_TEST_TIME_ZONE`` runs the suite in another
# zone, which is how a test that wrote Chicago's offset into a constant is
# found.
TIME_ZONE = os.environ.get("DDE_TEST_TIME_ZONE", "America/Chicago")

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
elif os.environ.get("DDE_TEST_DATABASE") == "mysql":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.mysql",
            "NAME": os.environ.get("MYSQL_DATABASE", "django_domain_events"),
            "USER": os.environ.get("MYSQL_USER", "root"),
            "PASSWORD": os.environ.get("MYSQL_PASSWORD", ""),
            "HOST": os.environ.get("MYSQL_HOST", "127.0.0.1"),
            "PORT": os.environ.get("MYSQL_PORT", "3306"),
            "OPTIONS": {"charset": "utf8mb4"},
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
