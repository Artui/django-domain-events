from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Mapping
from datetime import datetime, timezone
from typing import Any

from django_domain_events.declaration.registry import registry
from django_domain_events.settings import setting
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_status import DeliveryStatus

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


def lane_pauses(lanes: Collection[str], moment: datetime) -> dict[str, datetime]:
    """The latest pause of each of ``lanes`` still unexpired at ``moment``, by any writer."""
    from django_domain_events.models.lane_pause_record import LanePauseRecord

    held: dict[str, datetime] = {}
    rows = LanePauseRecord.objects.filter(until__gt=moment, lane__in=sorted(lanes))
    for lane, until in rows.values_list("lane", "until"):
        held[lane] = max(until, held.get(lane, until))
    return held


def driver_listens() -> bool:
    """Whether Django's Postgres backend loaded psycopg 3, the driver the wake listens through."""
    from django.db.backends.postgresql import psycopg_any

    return psycopg_any.is_psycopg3


# The name operators already filter a failed attempt's lines on, kept when the helpers moved here.
_failure_logger = logging.getLogger("django_domain_events.delivery.deliver")

# Strictly before any moment a claim can be made at, on any worker's clock. The
# claim's lapsed-lease arm is a strict ``lease_expires_at < now``, so a lease
# ended "now" is still held by a claim made in the same instant - which a test
# with a frozen clock, or a worker on a clock a little behind this one, is. The
# epoch also reads as what it is in an admin listing: not a lease that ran out,
# but one that was given back, and the claim spends no attempt re-claiming it.
# Aware, and converted where it is written: a project on ``USE_TZ = False``
# stores naive local time, and SQLite refuses an aware value there
# (``test_handing_back_writes_a_lease_the_backend_accepts``).
GIVEN_BACK = datetime(1970, 1, 1, tzinfo=timezone.utc)


def report_failure(
    row: Any, *, status: DeliveryStatus, attempt: int, error: str, error_type: str | None
) -> None:
    """Log a failed attempt this worker recorded, then tell its receiver."""
    log_failure(row, status=status, attempt=attempt, error_type=error_type)
    notify_failure(row, status=status, attempt=attempt, error=error)


def log_failure(row: Any, *, status: DeliveryStatus, attempt: int, error_type: str | None) -> None:
    """The one warning line a failed attempt writes, in one shape everywhere."""
    _failure_logger.warning(
        "receiver %s failed delivery %s on attempt %s with %s; the row is %s",
        row.receiver_key,
        row.pk,
        attempt,
        error_type if error_type is not None else "no exception",
        status,
    )


def notify_failure(row: Any, *, status: DeliveryStatus, attempt: int, error: str) -> None:
    """Tell the receiver its attempt failed; ``fire`` imports this module through ``wake``."""
    from django_domain_events.delivery.fire import run_to_completion

    receiver = registry.receiver_for_key(row.receiver_key)
    if receiver is None or receiver.on_failure is None:
        return
    try:
        run_to_completion(
            receiver.on_failure,
            DeliveryFailure(
                delivery_id=row.pk,
                event_id=row.event_id,
                event_name=row.event.name,
                receiver_key=row.receiver_key,
                attempt=attempt,
                status=status,
                error=error,
                target=row.target,
            ),
        )
    except Exception:
        _failure_logger.exception(
            "on_failure hook for receiver %s raised while recording delivery %s; "
            "the delivery row is correct and the hook's record is lost",
            row.receiver_key,
            row.pk,
        )
