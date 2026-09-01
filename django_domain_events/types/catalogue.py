from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.types.catalogue_event import CatalogueEvent


@dataclass(frozen=True, slots=True)
class Catalogue:
    """Every declared event and what listens to it, at one moment.

    A snapshot rather than a live view: it is built to be written to a file and
    compared against the one from last release, which is the whole point of
    having it.
    """

    events: tuple[CatalogueEvent, ...]
