from __future__ import annotations

from django.db import router


def write_alias() -> str:
    """The database alias this package's rows are written to.

    Everything that opens a transaction or asks whether one is open has to name
    it. Hardcoding ``default`` breaks the guarantee the package is built on the
    moment a router sends the event log elsewhere: the atomic block guards one
    connection while every write goes to another in autocommit, so the receiver's
    work and the acknowledgement no longer commit together.
    """
    from django_domain_events.models.event_record import EventRecord

    return router.db_for_write(EventRecord) or "default"
