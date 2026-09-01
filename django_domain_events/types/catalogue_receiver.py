from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CatalogueReceiver:
    """One receiver of one event, as the catalogue describes it."""

    key: str
    callable_path: str
    mode: str
    site: str
    max_attempts: int
    eager: bool
    takes_context: bool
