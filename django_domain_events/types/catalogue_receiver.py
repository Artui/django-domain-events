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

    backoff_base_seconds: float | None = None
    """The receiver's own retry curve, or None where it uses the setting.
    Defaulted, like every field after ``lease_seconds``."""

    backoff_cap_seconds: float | None = None
    """The ceiling of that curve, or None where it uses the setting."""

    lane: str = "default"
    """Which relay processes claim this receiver's rows: those started with
    ``--lane`` naming it, or, for ``"default"``, those started with none."""

    give_up_after_seconds: float | None = None
    """How long a delivery may stay owed while it defers without counting,
    in seconds, or None where the receiver declares no bound. Seconds rather
    than the declared ``timedelta`` so the JSON stays plain numbers, as the
    curve does."""
