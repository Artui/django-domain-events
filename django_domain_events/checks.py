from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

from django.conf import settings
from django.core.checks import Error, Warning
from django.db import models
from django.utils.module_loading import import_string

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.registry import registry
from django_domain_events.settings import (
    DEFAULTS,
    SETTINGS_NAME,
    WAKE_MODES,
    get_codec,
    setting,
)
from django_domain_events.utils import has_table, owed

# The names a reader reaches for instead: the app label, and the prose everyone
# writes. Neither is read, and neither fails.
_NEAR_MISS_SETTINGS_NAMES = ("DOMAIN_EVENTS", "DJANGO_DOMAIN_EVENT", "DOMAIN_EVENT")


def check_receivers_have_events(**kwargs: Any) -> list[Any]:
    """Every receiver listens for something that was declared.

    Nothing else would say so: the event cannot be fired, so there is no failure
    to observe, only silence.

    A wildcard is exempt: ``AnyEvent`` is a marker, never declared as an event,
    and a receiver for it listens to everything that is.
    """
    problems = []
    for receiver in registry.receivers():
        if (
            receiver.event_class is not AnyEvent
            and registry.event_for_class(receiver.event_class) is None
        ):
            problems.append(
                Error(
                    f"Receiver {receiver.key!r} listens for "
                    f"{receiver.event_class.__name__}, which is not registered.",
                    hint="Decorate the event class with @event, or delete the receiver.",
                    id="django_domain_events.E001",
                )
            )
    return problems


def check_codec_dependency_is_installed(**kwargs: Any) -> list[Any]:
    """The configured codec can be imported.

    At startup rather than when an event fires: a codec is imported lazily, so
    the first symptom of a missing extra would otherwise be a failed delivery.
    """
    path = setting("CODEC")
    try:
        import_string(path)
    except ImportError as exc:
        return [
            Error(
                f"CODEC is set to {path!r}, which cannot be imported: {exc}",
                hint=(
                    "DaciteCodec needs the 'dacite' extra: "
                    "pip install 'django-domain-events[dacite]'"
                ),
                id="django_domain_events.E002",
            )
        ]
    return []


def check_declared_events_are_decodable(**kwargs: Any) -> list[Any]:
    """Every declared event can be rebuilt by the configured codec.

    The companion to
    :func:`check_codec_dependency_is_installed`, and the one that catches the
    failure that actually happens. That check asks whether the codec can be
    *imported*; this one asks whether it can decode the events this project
    declares -- which is a different question, and the one whose answer is
    silent when it is no.

    The asymmetry is what makes it worth a check. ``fire()`` encodes and commits
    inside the caller's transaction whatever the annotation says, so an event
    the codec cannot rebuild is recorded successfully and then dead-letters on
    every durable delivery, in the relay, in another process, possibly hours
    later. Nothing before that point fails.

    A codec that does not implement ``unsupported_fields`` is not interrogated:
    this package should not guess at what a codec it did not write can do.
    """
    codec = get_codec()
    inspect = getattr(codec, "unsupported_fields", None)
    if inspect is None:
        return []

    problems: list[Any] = []
    for registered in registry.events():
        for field_name, annotation in inspect(registered.event_class):
            problems.append(
                Error(
                    f"{registered.name}.{field_name} is annotated "
                    f"{annotation!r}, which {type(codec).__name__} cannot rebuild. "
                    "The event would be recorded and every durable delivery of it "
                    "would dead-letter.",
                    hint=(
                        "Nested dataclasses need the 'dacite' extra and CODEC set to "
                        "'django_domain_events.codecs.dacite_codec.DaciteCodec'. "
                        "Otherwise use a type the codec handles: str, int, float, bool, "
                        "Decimal, UUID, datetime, date, time, enums, literals, optionals, "
                        "and lists or tuples of those."
                    ),
                    id="django_domain_events.E005",
                )
            )
    return problems


def check_settings_keys_are_known(**kwargs: Any) -> list[Any]:
    """The settings dict is named correctly, holds no unrecognised keys, and the
    wake settings hold values the relay can act on.

    Both halves are silent by default. ``setting()`` reads only the keys this
    package asks for, so a typo sits in the settings looking effective; and the
    dict is ``DJANGO_DOMAIN_EVENTS`` while the app label, the import path and
    most prose say ``domain events``, so ``DOMAIN_EVENTS`` returns ``{}`` and
    every value falls back to its default with nothing said.

    That second case is not hypothetical: it is how a consumer configured
    ``CODEC`` correctly, saw no effect, and spent a round debugging a decode
    failure with the fix visibly in place.
    """
    problems: list[Any] = []

    for name in _NEAR_MISS_SETTINGS_NAMES:
        if hasattr(settings, name):
            problems.append(
                Warning(
                    f"settings.{name} is set, but this package reads "
                    f"{SETTINGS_NAME!r}. Nothing in it is being used.",
                    hint=f"Rename it to {SETTINGS_NAME}.",
                    id="django_domain_events.W006",
                )
            )

    configured = getattr(settings, SETTINGS_NAME, {})
    unknown = sorted(set(configured) - set(DEFAULTS))
    if unknown:
        problems.append(
            Warning(
                f"{SETTINGS_NAME} has unrecognised key(s): {', '.join(unknown)}. They are ignored.",
                hint=f"Valid keys are: {', '.join(sorted(DEFAULTS))}.",
                id="django_domain_events.W007",
            )
        )

    # Folded in here rather than registered on its own: it is the same silent
    # ineffective configuration, and the check is already wired.
    problems.extend(_wake_setting_problems())
    problems.extend(_relay_prune_setting_problems())
    return problems


def _wake_setting_problems() -> list[Any]:
    """``WAKE`` and ``NOTIFY_COALESCE_SECONDS`` hold values the relay can use.

    A misspelt ``WAKE`` would otherwise read as "not notify" and quietly turn
    NOTIFY off, and a negative or NaN interval compares false against every
    elapsed time, so it would silence every notification after the first.
    ``bool`` is refused because ``True`` is an ``int`` and would read as a
    one-second interval.

    The interval guard is one branch arc of three disjuncts, each held by one case
    of ``test_a_coalesce_interval_that_is_not_a_duration_is_an_error``: the bool
    test by ``True``, the type test by ``"0.5"`` and ``None``, and the sign test
    by ``nan`` (a ``-1`` passes under either form of it).
    """
    problems: list[Any] = []
    wake = setting("WAKE")
    if wake not in WAKE_MODES:
        problems.append(
            Error(
                f"WAKE is {wake!r}, which is not a wake mode.",
                hint=f"Use one of: {', '.join(repr(mode) for mode in WAKE_MODES)}.",
                id="django_domain_events.E006",
            )
        )
    interval = setting("NOTIFY_COALESCE_SECONDS")
    # ``not interval >= 0`` rather than ``interval < 0``: NaN fails the first
    # comparison and passes the second.
    if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not interval >= 0:
        problems.append(
            Error(
                f"NOTIFY_COALESCE_SECONDS is {interval!r}, which is not a number of "
                "seconds, zero or more.",
                hint="Use 0 to send every notification, or a positive number of seconds.",
                id="django_domain_events.E007",
            )
        )
    return problems


def _relay_prune_setting_problems() -> list[Any]:
    """``RELAY_PRUNE`` is a switch and ``RELAY_PRUNE_SECONDS`` a positive interval.

    Both fail the relay quietly if misread. A ``RELAY_PRUNE`` of ``"false"`` is
    a truthy string and would leave the sweep on for the operator who meant it
    off, and an interval of zero, a negative or NaN compares true or false
    against every elapsed time and turns the throttle into a sweep on every idle
    pass, or into none. Infinity is refused too: it is the off switch spelt
    badly, and ``RELAY_PRUNE`` is the spelling.

    The interval guard is one branch arc of several conditions, each held by a
    case of ``test_a_prune_interval_that_is_not_a_positive_duration_is_an_error``:
    the bool test by ``True``, the type test by ``"60"`` and ``None``, the sign
    test by ``0`` and ``-1``, the finiteness test by ``inf``, and the spelling
    ``interval > 0`` rather than ``interval <= 0`` by ``nan``, which is false
    against both.
    """
    problems: list[Any] = []
    switch = setting("RELAY_PRUNE")
    if not isinstance(switch, bool):
        problems.append(
            Error(
                f"RELAY_PRUNE is {switch!r}, which is not a bool.",
                hint="Use True to let an idle relay prune, or False to schedule prune_events.",
                id="django_domain_events.E008",
            )
        )
    interval = setting("RELAY_PRUNE_SECONDS")
    if (
        isinstance(interval, bool)
        or not isinstance(interval, (int, float))
        or not (interval > 0 and math.isfinite(interval))
    ):
        problems.append(
            Error(
                f"RELAY_PRUNE_SECONDS is {interval!r}, which is not a finite number of "
                "seconds above zero.",
                hint="Set RELAY_PRUNE to False to stop the relay pruning rather than a long interval.",
                id="django_domain_events.E009",
            )
        )
    return problems


def check_no_orphaned_deliveries(
    *, databases: Sequence[str] | None = None, **kwargs: Any
) -> list[Any]:
    """No delivery still owed names a receiver the registry no longer has.

    Owed means "not terminal", the same definition the relay claims by and the
    prune settles by, phrased as the owed partial indexes' own conditions
    (``owed``) so this reads the owed rows rather than every delivery ever
    made - it runs on every ``migrate`` and ``check``. A hand-written list of
    owed statuses is how this check once came to omit CLAIMED: a worker that
    died between claiming a row and the deploy that deleted its receiver leaves
    the row claimed with a lapsed lease, and it read as settled until a relay
    happened to reclaim it. That is now held by a test with a row in every
    status (test_the_orphan_warning_counts_every_owed_status_and_no_other).

    Two guards, and both are load-bearing. Without the first this runs under
    ``check``, ``showmigrations`` and ``makemigrations``, which pass no
    databases. Without the second it runs under ``migrate`` - which does pass
    one - and queries a table migrate has not created yet, so the first command
    a new project runs dies and no tables are created at all.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    if not databases:
        return []

    table = DeliveryRecord._meta.db_table
    keys: set[str] = set()
    for alias in databases:
        if not has_table(alias, table):
            continue
        keys |= set(
            DeliveryRecord.objects.using(alias)
            .filter(owed())
            .values_list("receiver_key", flat=True)
            .distinct()
        )
    missing = sorted(k for k in keys if registry.receiver_for_key(k) is None)
    if not missing:
        return []
    return [
        Warning(
            f"Delivery rows are waiting for receivers that no longer exist: {', '.join(missing)}.",
            hint=(
                "Restore the receiver under its old key=, or let the rows resolve "
                "to ORPHANED on the next delivery pass."
            ),
            id="django_domain_events.W001",
        )
    ]


def check_recorded_events_are_declared(
    *, databases: Sequence[str] | None = None, **kwargs: Any
) -> list[Any]:
    """No event still owed names something the registry cannot decode.

    Renaming an event is the case this catches and the orphan warning cannot:
    the receivers keep their keys, so nothing looks orphaned, while every row
    written under the old name now decodes to nothing and dead-letters one
    attempt budget at a time.

    Limited to rows still owed. A settled row naming a retired event is
    history, and warning about history every time ``check`` runs teaches the
    reader to skip the output.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    if not databases:
        return []

    table = EventRecord._meta.db_table
    # Phrased as the owed partial indexes' conditions, like the orphan check,
    # so Postgres can find the owed rows through those indexes and join out to
    # their events rather than probe every event's deliveries.
    owed_deliveries = DeliveryRecord.objects.filter(owed(), event=models.OuterRef("pk"))
    names: set[str] = set()
    for alias in databases:
        # One table answers for both: they are created by the same
        # migration, so neither exists without the other.
        if not has_table(alias, table):
            continue
        names |= set(
            EventRecord.objects.using(alias)
            .filter(models.Exists(owed_deliveries))
            .values_list("name", flat=True)
            .distinct()
        )
    missing = sorted(n for n in names if registry.event_for_name(n) is None)
    if not missing:
        return []
    return [
        Warning(
            f"Deliveries are owed for events no longer declared: {', '.join(missing)}.",
            hint=(
                "Most often a renamed event. Pin the old identity with "
                "@event(name=...) on the class that replaced it, or replay the "
                "rows under the new name."
            ),
            id="django_domain_events.W002",
        )
    ]
