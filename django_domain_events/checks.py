from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

from django.conf import settings
from django.core.checks import Error, Warning
from django.db import connections, models
from django.utils.module_loading import import_string

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.utils import driver_listens
from django_domain_events.delivery.write_alias import write_alias
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


def check_wake_driver_can_listen(**kwargs: Any) -> list[Any]:
    """``WAKE = "notify"`` on Postgres has a driver that can listen.

    The wake listens through psycopg 3, so under psycopg2 the relay polls and
    ``fire()`` sends no NOTIFY. That is correct, only slower to deliver, and
    ``notify`` is the default, so a psycopg2 project would otherwise be running
    a mode it never chose and never be told. A warning rather than an error for
    the same reason: nothing is lost.

    Of the write connection, because that is the one the relay listens on.
    Reading its ``vendor`` opens no connection, so this needs no database tag.
    The same predicate as the wake's own (``wake._resolve``), restated here
    rather than shared through it: the wake's answer folds in the ``supported``
    override, which a check has no use for.

    One branch arc of three conjuncts, each held by a test that fails without
    it: the vendor by
    ``test_a_backend_without_notify_is_not_warned_about_its_driver``, ``WAKE`` by
    ``test_poll_on_a_driver_that_cannot_listen_is_clean`` and the driver by
    ``test_notify_on_psycopg3_is_clean``.
    """
    if (
        connections[write_alias()].vendor != "postgresql"
        or setting("WAKE") != "notify"
        or driver_listens()
    ):
        return []
    return [
        Warning(
            "WAKE is 'notify', but the database driver is psycopg2, which cannot "
            "listen for a notification. The relay polls every POLL_SECONDS "
            "instead, and fire() sends no NOTIFY.",
            hint=(
                'Set WAKE = "poll" in DJANGO_DOMAIN_EVENTS to say so and silence '
                "this, or install psycopg 3 to have the relay woken."
            ),
            id="django_domain_events.W008",
        )
    ]


def check_database_measures_characters(
    *, databases: Sequence[str] | None = None, **kwargs: Any
) -> list[Any]:
    """The write database counts a ``varchar``'s length in characters.

    Every length this package holds a string to - the refusals of a long
    receiver key, event name, ``dedupe_key`` and suppression reason, and the
    truncation of the actor's two strings at capture - counts characters,
    because that is what a Postgres ``varchar(n)`` counts. A ``SQL_ASCII``
    database is the exception: it has no encoding to decode with, so it counts
    bytes, and a value of 255 characters that are not all ASCII passes every
    one of those checks and is then refused by the insert with ``DataError``,
    inside the caller's transaction. Counting bytes everywhere instead would
    refuse keys a UTF8 database stores, and the declaration-time refusals run
    before any connection exists, so the database is named here rather than
    measured in each of them.

    A warning, because an ASCII-only project on such a database loses nothing.
    Under the database tag, as the query needs a connection: ``migrate`` runs
    it, and ``check --database`` does. Two guards, each held by a test that
    fails without it: the databases asked about
    (``test_an_encoding_check_not_asked_about_the_database_reads_nothing``) and
    the vendor (``test_a_backend_other_than_postgres_is_not_asked_its_encoding``).
    """
    alias = write_alias()
    if not databases or alias not in databases:
        return []
    if connections[alias].vendor != "postgresql" or _server_encoding(alias) != "SQL_ASCII":
        return []
    return [
        Warning(
            f"The database behind {alias!r} is SQL_ASCII, which measures a varchar in "
            "bytes. This package's 255-character limits count characters, so a "
            "non-ASCII receiver key, event name, dedupe_key, suppression reason or "
            "actor label within them can still be refused by the insert with "
            "DataError.",
            hint=(
                "Move the event tables to a UTF8 database, or keep those strings "
                "ASCII. PostgreSQL itself advises against SQL_ASCII for non-ASCII data."
            ),
            id="django_domain_events.W009",
        )
    ]


def _server_encoding(alias: str) -> str:
    """The encoding Postgres stores this database's text in, as it spells it."""
    with connections[alias].cursor() as cursor:
        cursor.execute("SHOW server_encoding")
        (encoding,) = cursor.fetchone()
    return encoding


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
    problems.extend(_around_delivery_problems())
    problems.extend(_relay_prune_setting_problems())
    problems.extend(_prune_batch_problems())
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


def _around_delivery_problems() -> list[Any]:
    """``AROUND_DELIVERY`` is None, or a dotted path to something callable.

    Checked here because the path is resolved on every delivery and nowhere
    else: a typo would otherwise surface as the first durable delivery failing,
    and every one after it, in the relay, possibly hours after the deploy.

    The value is refused for three reasons, each held by one case of
    ``test_a_hook_that_cannot_be_used_is_an_error``: not a string at all, by
    the callable itself (the likeliest slip, since collaborators elsewhere are
    objects, and ``import_string`` would raise out of the check rather than
    report); not importable, by a missing attribute and a missing module; and
    not callable, by a module constant.
    """
    path = setting("AROUND_DELIVERY")
    if path is None:
        return []
    if not isinstance(path, str):
        problem = f"is {path!r}, which is not a dotted path"
    else:
        try:
            factory = import_string(path)
        except ImportError as exc:
            problem = f"is {path!r}, which cannot be imported: {exc}"
        else:
            if callable(factory):
                return []
            problem = f"is {path!r}, which is not callable"
    return [
        Error(
            f"AROUND_DELIVERY {problem}. Every durable delivery would fail on entering it.",
            hint=(
                "Name a callable that takes a DeliveryContext and returns a context "
                "manager, by its dotted path, or set it to None."
            ),
            id="django_domain_events.E011",
        )
    ]


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
    test by ``0`` and ``-1``, and the finiteness test by ``inf`` and ``nan``
    (which is false against ``> 0`` too, so either conjunct refuses it).
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


def _prune_batch_problems() -> list[Any]:
    """``PRUNE_BATCH_ROWS`` is a positive whole number of rows.

    ``prune_events`` refuses anything else with a ``ValueError``, which an idle
    relay's sweep logs and swallows once an interval: the setting would be
    wrong everywhere and the only sign a line in a log. The same bool-first
    test as the prune's own, since ``True`` is an ``int``.
    """
    size = setting("PRUNE_BATCH_ROWS")
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        return [
            Error(
                f"PRUNE_BATCH_ROWS is {size!r}, which is not a positive whole number of rows.",
                hint="Use the number of rows one prune transaction may delete; the default is 5000.",
                id="django_domain_events.E010",
            )
        ]
    return []


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
