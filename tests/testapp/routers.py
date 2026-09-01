from __future__ import annotations


class EventsRouter:
    """Sends this package's models to the ``events`` database."""

    def db_for_read(self, model, **hints):
        return "events" if model._meta.app_label == "django_domain_events" else None

    def db_for_write(self, model, **hints):
        return "events" if model._meta.app_label == "django_domain_events" else None

    def allow_migrate(self, db, app_label, **hints):
        if app_label == "django_domain_events":
            return db == "events"
        return db == "default"
