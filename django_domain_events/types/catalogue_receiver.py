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
    lease_seconds: int | None = None
    """Defaulted so adding it does not break a consumer constructing one."""

    targets: str | None = None
    """Where a fan-out receiver's ``targets`` callable lives, or None for a
    receiver that writes one delivery per event. Published because it changes
    what a delivery *is* for this receiver - one per target rather than one per
    event - and a reader diffing catalogues should see that change. Defaulted
    for the same reason as ``lease_seconds``."""
