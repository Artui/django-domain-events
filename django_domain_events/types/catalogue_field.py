from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CatalogueField:
    """One field of an event payload, as the catalogue describes it."""

    name: str
    type: str
    required: bool
    """False when the field has a default, which is also the condition under
    which adding it is a non-breaking change to a log with rows already in it."""

    default: str | None
