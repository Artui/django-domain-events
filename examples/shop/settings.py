"""Settings for the example shop.

SQLite by default so `python manage.py demo` runs with no setup. Set
DDE_EXAMPLE_DATABASE=postgres for the half SQLite cannot show: more than one
relay over one queue needs SELECT ... FOR UPDATE SKIP LOCKED, which SQLite has
neither the statement nor the concurrency model for.
"""

import getpass
import os

SECRET_KEY = "not-a-secret-this-is-an-example"

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.messages",
    "django.contrib.sessions",
    "django.contrib.admin",
    "django_domain_events",
    "shop",
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

ROOT_URLCONF = "urls"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

if os.environ.get("DDE_EXAMPLE_DATABASE") == "postgres":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("PGDATABASE", "dde_example_shop"),
            "USER": os.environ.get("PGUSER", getpass.getuser()),
            "PASSWORD": os.environ.get("PGPASSWORD", ""),
            "HOST": os.environ.get("PGHOST", "localhost"),
        }
    }
else:
    DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": "shop.sqlite3"}}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    # The relay logs at WARNING when a worker loses a delivery, and names the
    # receiver whose lease_seconds= to raise.
    "loggers": {"django_domain_events": {"handlers": ["console"], "level": "WARNING"}},
}

DJANGO_DOMAIN_EVENTS = {
    "LEASE_SECONDS": 30,
    "RETENTION_DAYS": 30,
}
