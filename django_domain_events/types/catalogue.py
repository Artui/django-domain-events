from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.types.catalogue_event import CatalogueEvent
from django_domain_events.types.catalogue_receiver import CatalogueReceiver


@dataclass(frozen=True, slots=True)
class Catalogue:
    """Every declared event and what listens to it, at one moment."""

    events: tuple[CatalogueEvent, ...]

    wildcard_receivers: tuple[CatalogueReceiver, ...] = ()
    """Receivers declared for ``AnyEvent``, which receive every event above.

    A section of their own rather than a row under every event: a transport
    listed under all two hundred events is a catalogue nobody reads, and it
    says nothing about any one of them. Defaulted so adding it does not break a
    consumer constructing one."""
