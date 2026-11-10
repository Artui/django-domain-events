from __future__ import annotations

from django.db import router


def write_alias() -> str:
    """The database alias this package's rows are written to."""
    from django_domain_events.models.event_record import EventRecord

    return router.db_for_write(EventRecord) or "default"
