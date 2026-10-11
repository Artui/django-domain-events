from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.types.catalogue_field import CatalogueField
from django_domain_events.types.catalogue_receiver import CatalogueReceiver


@dataclass(frozen=True, slots=True)
class CatalogueEvent:
    """One declared event, its payload shape and everything listening to it."""

    name: str
    version: int
    class_path: str
    doc: str
    fields: tuple[CatalogueField, ...]
    receivers: tuple[CatalogueReceiver, ...]

    migrates_older_rows: bool = False
    """Whether the class declares ``upgrade``. Worth publishing: it is the
    difference between an event whose old rows still decode and one whose old
    rows dead-letter after the next breaking change.

    Last, and defaulted, so adding it does not break a consumer constructing
    one - these are exported types."""

    retention_seconds: int | None = None
    """The window of its own the event is declared with, in seconds, or None.

    Published as ``fire()`` records it on the row rather than as the declared
    ``timedelta`` or ``Retention``, so the JSON form stays plain values, and a
    pipeline diffing catalogues sees an event start or stop deleting early.
    Defaulted for the same reason as ``migrates_older_rows``."""

    delete_when: str = ""
    """``"succeeded"`` or ``"settled"`` for an event deleted once consumed (the
    ``Retention`` value), or blank. At most one of this and
    ``retention_seconds`` is set; with neither, the event follows
    ``RETENTION_DAYS``."""
