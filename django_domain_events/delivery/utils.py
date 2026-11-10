from __future__ import annotations

from collections.abc import Callable, Collection, Mapping

from django_domain_events.declaration.registry import registry
from django_domain_events.settings import setting

OnDeferral = Callable[[str, float], None]
"""Told the lane and the requested delay, in seconds, of a deferral that did
not count and was recorded. The relay pauses that lane on hearing it."""


def claim_size(batch_size: int | None, *, name: str = "batch_size") -> int:
    """The rows one claim takes: ``batch_size`` if given, else ``BATCH_SIZE``."""
    if batch_size is None:
        return setting("BATCH_SIZE")
    if batch_size < 1:
        raise ValueError(f"{name} must be positive, got {batch_size}")
    return batch_size


def served_lanes(lane: str | Collection[str] | None) -> tuple[str, ...] | None:
    """The lanes a relay serves, validated and sorted, or None for every lane."""
    if lane is None:
        return None
    members = [lane] if isinstance(lane, str) else list(lane)
    if not members:
        raise ValueError(
            "lane is an empty collection, which serves no lane: name at least one, "
            "or pass None for every lane."
        )
    for member in members:
        if not isinstance(member, str):
            raise ValueError(f"A lane is named by a str, got {member!r}.")
        registry.require_lane(member)
    return tuple(sorted(set(members)))


def lane_claim_sizes(
    lanes: tuple[str, ...] | None,
    *,
    batch_size: int | None,
    lane_batch_sizes: Mapping[str, int] | None,
) -> Callable[[str], int]:
    """How many rows a claim of each lane takes, resolved once at start."""
    default = claim_size(batch_size)
    served = registry.lanes() if lanes is None else lanes
    overrides: dict[str, int] = {}
    for name, size in sorted((lane_batch_sizes or {}).items()):
        if name not in served:
            raise ValueError(
                f"lane_batch_sizes names lane {name!r}, which is not served here. "
                f"Served lanes: {', '.join(served)}."
            )
        overrides[name] = claim_size(size, name=f"lane_batch_sizes[{name!r}]")
    return lambda name: overrides.get(name, default)


def driver_listens() -> bool:
    """Whether Django's Postgres backend loaded psycopg 3, the driver the wake listens through."""
    from django.db.backends.postgresql import psycopg_any

    return psycopg_any.is_psycopg3
