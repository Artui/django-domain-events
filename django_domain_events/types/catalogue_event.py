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
