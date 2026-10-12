"""Settings for the example shop.

SQLite by default so `python manage.py demo` runs with no setup. Set
DDE_EXAMPLE_DATABASE=postgres for the half SQLite cannot show: more than one
relay over one queue needs SELECT ... FOR UPDATE SKIP LOCKED, which SQLite has
neither the statement nor the concurrency model for.
"""

from __future__ import annotations

import getpass
import os

SECRET_KEY = "not-a-secret-this-is-an-example"

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.messages",
    "django.contrib.sessions",
    "django.contrib.staticfiles",
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
STATIC_URL = "static/"
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
    # The filter puts the delivery and receiver ids the AROUND_DELIVERY hook
    # set on every line logged during a delivery, and "-" on every other line.
    "filters": {"delivery": {"()": "shop.delivery_log.DeliveryLogFilter"}},
    "formatters": {"delivery": {"format": "   %(levelname)s [%(delivery)s] %(name)s: %(message)s"}},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "filters": ["delivery"],
            "formatter": "delivery",
        }
    },
    "loggers": {
        # The relay logs at WARNING when a worker loses a delivery, and names
        # the receiver whose lease_seconds= to raise.
        "django_domain_events": {"handlers": ["console"], "level": "WARNING"},
        "shop": {"handlers": ["console"], "level": "INFO"},
    },
}

DJANGO_DOMAIN_EVENTS = {
    "LEASE_SECONDS": 30,
    "RETENTION_DAYS": 30,
    # Entered around every durable attempt; see shop/delivery_log.py.
    "AROUND_DELIVERY": "shop.delivery_log.delivery_log_context",
}
