from __future__ import annotations

import dataclasses
import hashlib
import inspect
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any, cast

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
    """Not terminal, phrased as the two owed partial indexes' own conditions.

    The rows ``exclude(status__in=TERMINAL)`` means, in the one form Postgres
    matches to those indexes: each arm implies one index's condition, and the
    OR is answered by OR-ing the two indexes' bitmaps, so a query filtering on
    this reads the owed rows and nothing of the delivered history.

    Listing the owed statuses gives up what "not terminal" had: it could not
    miss a status added later, and a hand-written list once omitted CLAIMED.
    Both are held by tests rather than by the phrasing - one row in every
    status, owed exactly when not terminal
    (test_every_status_is_counted_owed_or_settled_exactly_once, and
    test_the_orphan_warning_counts_every_owed_status_and_no_other).
    """
    return models.Q(status__in=RETRYABLE) | models.Q(status=DeliveryStatus.CLAIMED)


def not_yet_due(now: datetime) -> models.Q:
    """Rows whose due time is still ahead of ``now``: scheduled, not yet owed.

    Read off ``due_at`` and never ``available_at``, because only the first
    tells a schedule from a backoff: the backoff moves ``available_at`` ahead on
    every failed attempt and leaves ``due_at`` alone, so a row failing after
    it came due has a future ``available_at`` and a past ``due_at``, and is
    owed. A NULL ``due_at`` is a row owed since its event was recorded, which is
    never in the future, so it is never matched here.

    Strictly after ``now``, as the claim takes a row once ``available_at <=
    now``: at its due time a row is owed, to the claim and to health alike
    (``test_a_row_becomes_owed_at_exactly_its_due_time``).

    A filter on rows already narrowed to the owed ones, not a predicate of its
    own: it adds nothing an index could use, so every caller applies it to a
    query whose WHERE clause is ``owed()``, and the owed partial indexes are
    what read the rows.
    """
    return models.Q(due_at__gt=now)


def label_for(module: str, fallback_name: str) -> str:
    """Build a ``<app_label>.<name>`` identity for a declaration.

    The app label rather than the dotted import path: the path is a refactor
    away from orphaning every pending row that names it.
    """
    config = apps.get_containing_app_config(module)
    if config is None:
        raise LookupError(
            f"{module} is not inside an installed app, so no stable name can be "
            f"derived for {fallback_name!r}. Pass an explicit name= or key=."
        )
    return f"{config.label}.{fallback_name}"


def require_frozen_dataclass(event_class: type) -> None:
    """Reject an event class that cannot behave like a recorded value."""
    if not dataclasses.is_dataclass(event_class):
        raise TypeError(
            f"{event_class.__name__} is not a dataclass. Events are declared as "
            f"frozen dataclasses so they can be encoded and rebuilt."
        )
    # __dataclass_params__ is written at runtime and absent from the stdlib
    # protocol, so the checker cannot see what is_dataclass has established.
    if not cast(Any, event_class).__dataclass_params__.frozen:
        raise TypeError(
            f"{event_class.__name__} is a mutable dataclass. Declare it "
            f"frozen=True: at-least-once delivery hands a different instance to "
            f"every attempt, so a receiver mutating one writes to a copy."
        )


def parse_datetime(value: str) -> datetime:
    """Parse a datetime the way ``DjangoJSONEncoder`` writes one.

    The encoder emits UTC with a trailing ``Z``, which ``fromisoformat`` only
    learned to read in Python 3.11. Bare parsing fails on the 3.10 floor.
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def in_project_time(value: datetime) -> datetime:
    """``value`` in the form the project stores datetimes in.

    Every datetime the package compares or writes is either one it took from
    ``django.utils.timezone.now()`` or one the ORM read back, and both follow
    ``USE_TZ``: aware under ``True``, naive ``TIME_ZONE`` local time under
    ``False``. A value an application hands in - a ``now=``, a ``NotYet``, a
    ``due=`` answer, ``fire(occurred_at=)`` - goes through here first, so it
    meets those on their own terms: comparing an aware value with a naive one
    raises ``TypeError``, and SQLite refuses to store an aware one at all under
    ``USE_TZ = False``.

    An aware value under ``False`` is converted to local time and stripped,
    never read off as UTC: a naive value from the database is local time, so a
    UTC wall clock beside it would be hours out
    (``test_a_due_callable_answering_an_aware_time_is_stored_as_local_time``).
    Everything else is returned as it came. A naive value under ``True`` is
    refused where it matters, by the caller, because only the caller can say
    what it was for.

    The guard is one arc of two conditions, so each has a test that fails
    without it: ``USE_TZ`` - ``test_it_carries_the_time_and_the_reason``
    (``NotYet`` would hand back local time under ``True``); naive -
    ``test_not_yet_takes_local_time_and_moves_the_row_there`` (``make_naive``
    raises on a naive value).
    """
    if settings.USE_TZ or timezone.is_naive(value):
        return value
    return timezone.make_naive(value)


def require_aware_where_stored_aware(value: datetime, refusal: str) -> None:
    """Refuse a naive datetime, unless the project stores naive ones.

    Under ``USE_TZ = True`` a naive value could mean UTC or local time, and
    reading it either way would be wrong by hours for somebody. Under ``False``
    naive is the project's own convention, local time, so it is no guess, and
    refusing it would refuse the value ``timezone.now()`` returns there.

    One arc of two conditions: ``USE_TZ`` is held by
    ``test_a_due_callable_may_answer_in_local_time`` and
    ``test_not_yet_takes_local_time_and_moves_the_row_there``, and naive by
    ``test_it_carries_the_time_and_the_reason``, which an aware value would
    fail under ``True`` without it.
    """
    if settings.USE_TZ and timezone.is_naive(value):
        raise ValueError(refusal)


def has_table(alias: str, table: str) -> bool:
    """Whether a table exists on one connection.

    Both database checks need it. Without it they run under ``migrate`` - which
    passes a database - and query a table ``migrate`` has not created yet, so
    the first command a new project runs dies and no tables are created at all.
    """
    connection = connections[alias]
    with connection.cursor() as cursor:
        return table in connection.introspection.table_names(cursor)


def require_valid_upgrade(event_class: type) -> None:
    """Reject an ``upgrade`` hook that cannot be called the way we call it.

    At the decorator rather than in the relay hours later, which is the same
    reason the frozen check is here. A plain ``def upgrade(self, ...)`` is the
    trap worth naming: reached through the class it is an unbound function, so
    the payload would silently arrive as ``self``.
    """
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
    """Rebuild an event from a row, migrating an older payload first.

    Shared by the relay and by ``assert_fired`` so the two cannot decode the
    same row differently - a test helper that reads a payload the relay would
    reject is how a suite comes to agree with a bug.

    The hook runs only when the row is *older* than the declaration. A row from
    the future is a rollback, and no forward migration helps: the code that
    would know how to read it is the code that was just removed.
    """
    # Function-local, and only this one: settings imports the codec package,
    # which imports this module. registry has no such cycle and is imported at
    # the top.
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
            # The commonest way to write the hook wrong is to mutate in place
            # and forget the return. Saying so here is the difference between
            # naming the hook and dead-lettering with "argument of type
            # 'NoneType' is not iterable".
            raise PayloadUpgradeFailed(
                f"{event_class.__name__}.upgrade() returned {type(migrated).__name__}, "
                f"not a mapping. It has to return the migrated payload; mutating "
                f"the one it was given is not enough."
            )
        # The declared version, not the row's: the payload the codec is about
        # to see has been migrated, and a codec is handed the version so it can
        # branch on it. Telling it the old one asks for the old treatment of
        # new-shaped data.
        return get_codec().decode(event_class, dict(migrated), entry.version)
    return get_codec().decode(event_class, payload, version)


def resolve_targets(
    receiver_key: str,
    targets: Callable[[Any, DeliveryContext], Iterable[str]],
    event: object,
    context: DeliveryContext,
) -> list[str]:
    """Call a fan-out receiver's ``targets`` and check what came back.

    Shared by ``fire()`` and ``replay_events()``, so a replay cannot write a
    target that firing would have refused.

    Two refusals, each raised rather than skipped, because this runs inside a
    transaction somebody else opened and the alternative to raising is a
    delivery that silently never happens. A non-string is refused because the
    column would store its ``str()`` while a replay compares the original
    value, so the two would never match. An empty string is refused because it
    is what a receiver *without* targets writes, and a blank row from a fan-out
    would read as not being one.

    Duplicates are dropped, first occurrence kept: one delivery per target is
    the promise, and the unique constraint would otherwise turn a callable that
    joined its way to the same destination twice into a failed transaction.
    """
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
    """When a scheduled receiver's delivery of ``event`` comes due.

    Shared by ``fire()`` and ``replay_events()``, as ``resolve_targets`` is, so
    a replay cannot write a due time firing would have refused. A delay counts
    from ``occurred_at``; a callable is handed the event.

    Two refusals, raised rather than skipped, for the reason ``resolve_targets``
    gives: this runs inside somebody else's transaction, and the alternative is
    a reaction that silently never happens. A value that is not a datetime is
    refused before it reaches a comparison that would fail with a less useful
    message, and a naive one under ``USE_TZ = True`` because reading it as UTC
    or as local time would be wrong by hours for somebody. Under ``False`` a
    naive answer is local time, and an aware one is converted to it.

    Never earlier than ``floor`` - when the row is written: ``recorded_at`` at
    fire time, the moment of the replay on a replay. A due time already past
    is due at once, and ``due_at`` is when a row became owed, which cannot be
    before it existed: a backfilled event would otherwise read as a backlog
    that old the instant its row was written, and a ``give_up_after`` measured
    from it would dead-letter the first deferral
    (``test_a_due_time_already_past_is_due_when_the_row_is_written``).
    """
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
    """The digest a delivery row's uniqueness is enforced on, for one target.

    The one place it is computed: the model field derives it from here on every
    write, and replay looks existing rows up by it. SHA-256 of the UTF-8 text,
    as its 32 raw bytes, so every target - the blank one included - indexes as
    the same fixed width however long the text is.

    Raw rather than hex because the unique index holds one per delivery row:
    hex spelled the same value in twice the bytes, and on a large fan-out
    table that was the largest single thing on disk after the rows themselves.
    """
    return hashlib.sha256(target.encode()).digest()
