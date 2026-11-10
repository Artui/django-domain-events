from __future__ import annotations

import dataclasses
import hashlib
import inspect
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any, cast

from asgiref.sync import iscoroutinefunction
from django.apps import apps
from django.conf import settings
from django.db import connections, models
from django.utils import timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.payload_upgrade_failed import PayloadUpgradeFailed
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_status import DeliveryStatus

TERMINAL = (
    DeliveryStatus.SUCCEEDED,
    DeliveryStatus.DEAD,
    DeliveryStatus.ORPHANED,
    DeliveryStatus.CANCELLED,
)
"""Statuses a delivery does not come back from on its own.

Everything else is still owed. Named once because every reader of "settled"
asks the question and an explicit list of the owed statuses is what let two of
them disagree: one omitted CLAIMED, so a row whose worker died between the claim
and the deploy that deleted its receiver read as settled until a relay
happened to reclaim it. Replay and the event changelist's owed count kept
copies of their own until CANCELLED arrived, and now read this one.

Each member is held per reader, because membership in a tuple is one branch
arc: CANCELLED by test_a_cancelled_row_is_not_run (the relay's refusal),
test_a_cancelled_delivery_is_reopened_like_any_settled_one (replay),
test_a_cancelled_delivery_is_settled_for_the_ordinary_window (prune),
test_a_cancelled_delivery_is_not_owed (the admin) and
test_every_status_is_counted_owed_or_settled_exactly_once (health).
"""

RETRYABLE = (DeliveryStatus.PENDING, DeliveryStatus.FAILED)
"""The owed statuses the claim's first partial index is conditioned on.

Named so that the index and ``outbox_health``, which reads through it, spell
the predicate from one place: Postgres uses a partial index only for a query
whose WHERE clause implies the index's own condition, so a query that phrased
"owed" any other way - ``status NOT IN (terminal)`` is the natural one - reads
the whole table instead. CLAIMED is the other owed status, and has an index of
its own.
"""


def owed() -> models.Q:
    """Not terminal, phrased as the two owed partial indexes' own conditions."""
    return models.Q(status__in=RETRYABLE) | models.Q(status=DeliveryStatus.CLAIMED)


def not_yet_due(now: datetime) -> models.Q:
    """Rows whose due time is still ahead of ``now``: scheduled, not yet owed."""
    return models.Q(due_at__gt=now)


def label_for(module: str, fallback_name: str) -> str:
    """Build a ``<app_label>.<name>`` identity for a declaration."""
    config = apps.get_containing_app_config(module)
    if config is None:
        raise LookupError(
            f"{module} is not inside an installed app, so no stable name can be "
            f"derived for {fallback_name!r}. Pass an explicit name= or key=."
        )
    return f"{config.label}.{fallback_name}"


def is_async_callable(func: object) -> bool:
    """Whether calling ``func`` gives a coroutine to run rather than a result."""
    return iscoroutinefunction(func) or iscoroutinefunction(type(func).__call__)


def require_frozen_dataclass(event_class: type) -> None:
    """Reject an event class that cannot behave like a recorded value."""
    if not dataclasses.is_dataclass(event_class):
        raise TypeError(
            f"{event_class.__name__} is not a dataclass. Events are declared as "
            f"frozen dataclasses so they can be encoded and rebuilt."
        )
    if not cast(Any, event_class).__dataclass_params__.frozen:
        raise TypeError(
            f"{event_class.__name__} is a mutable dataclass. Declare it "
            f"frozen=True: at-least-once delivery hands a different instance to "
            f"every attempt, so a receiver mutating one writes to a copy."
        )


def parse_datetime(value: str) -> datetime:
    """Parse a datetime the way ``DjangoJSONEncoder`` writes one."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def in_project_time(value: datetime) -> datetime:
    """``value`` in the form the project stores datetimes in."""
    if settings.USE_TZ or timezone.is_naive(value):
        return value
    return timezone.make_naive(value)


def require_aware_where_stored_aware(value: datetime, refusal: str) -> None:
    """Refuse a naive datetime, unless the project stores naive ones."""
    if settings.USE_TZ and timezone.is_naive(value):
        raise ValueError(refusal)


def has_table(alias: str, table: str) -> bool:
    """Whether a table exists on one connection."""
    connection = connections[alias]
    with connection.cursor() as cursor:
        return table in connection.introspection.table_names(cursor)


def require_valid_upgrade(event_class: type) -> None:
    """Reject an ``upgrade`` hook that cannot be called the way we call it."""
    hook = inspect.getattr_static(event_class, "upgrade", None)
    if hook is None:
        return
    if not isinstance(hook, (staticmethod, classmethod)):
        raise TypeError(
            f"{event_class.__name__}.upgrade must be a staticmethod or a "
            f"classmethod. Reached through the class an ordinary method is "
            f"unbound, so the payload would arrive as its first argument."
        )
    try:
        inspect.signature(event_class.upgrade).bind({}, 1)
    except TypeError as exc:
        raise TypeError(
            f"{event_class.__name__}.upgrade must accept (payload, from_version): {exc}"
        ) from exc


def decode_payload(event_class: type, payload: dict[str, Any], version: int) -> Any:
    """Rebuild an event from a row, migrating an older payload first."""
    from django_domain_events.settings import get_codec

    entry = registry.event_for_class(event_class)
    hook = getattr(event_class, "upgrade", None)
    if hook is not None and entry is not None and version < entry.version:
        try:
            migrated = hook(payload, version)
        except Exception as exc:
            raise PayloadUpgradeFailed(
                f"{event_class.__name__}.upgrade() failed on a version {version} "
                f"payload: {type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(migrated, Mapping):
            raise PayloadUpgradeFailed(
                f"{event_class.__name__}.upgrade() returned {type(migrated).__name__}, "
                f"not a mapping. It has to return the migrated payload; mutating "
                f"the one it was given is not enough."
            )
        return get_codec().decode(event_class, dict(migrated), entry.version)
    return get_codec().decode(event_class, payload, version)


def resolve_targets(
    receiver_key: str,
    targets: Callable[[Any, DeliveryContext], Iterable[str]],
    event: object,
    context: DeliveryContext,
) -> list[str]:
    """Call a fan-out receiver's ``targets`` and check what came back."""
    resolved: dict[str, None] = {}
    for target in targets(event, context):
        if not isinstance(target, str):
            raise TypeError(
                f"targets for receiver {receiver_key!r} returned {target!r}, a "
                f"{type(target).__name__}. Targets are strings; convert ids with str()."
            )
        if not target:
            raise ValueError(
                f"targets for receiver {receiver_key!r} returned an empty string. A blank "
                f"target is what a receiver without targets= writes, so it cannot name one."
            )
        resolved[target] = None
    return list(resolved)


def resolve_due(
    receiver_key: str,
    due: timedelta | Callable[[Any], datetime],
    event: object,
    occurred_at: datetime,
    floor: datetime,
) -> datetime:
    """When a scheduled receiver's delivery of ``event`` comes due."""
    if isinstance(due, timedelta):
        when = occurred_at + due
    else:
        when = due(event)
        if not isinstance(when, datetime):
            raise TypeError(
                f"due for receiver {receiver_key!r} returned {when!r}, a "
                f"{type(when).__name__}. It must return a datetime."
            )
        require_aware_where_stored_aware(
            when,
            f"due for receiver {receiver_key!r} returned the naive datetime {when!r}. "
            f"Give it a timezone; under USE_TZ = True reading it as UTC or as local "
            f"time would each be a guess.",
        )
    return max(in_project_time(when), floor)


def target_digest(target: str) -> bytes:
    """The digest a delivery row's uniqueness is enforced on, for one target."""
    return hashlib.sha256(target.encode()).digest()
