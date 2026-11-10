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
    """Every receiver listens for something that was declared."""
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
    """The configured codec can be imported."""
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
    """``WAKE = "notify"`` on Postgres has a driver that can listen."""
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
    """The write database counts a ``varchar``'s length in characters."""
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
    """Every declared event can be rebuilt by the configured codec."""
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
    """The settings dict holds no unknown keys, and its wake values are usable by the relay."""
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

    problems.extend(_wake_setting_problems())
    problems.extend(_around_delivery_problems())
    problems.extend(_relay_prune_setting_problems())
    problems.extend(_prune_batch_problems())
    return problems


def _wake_setting_problems() -> list[Any]:
    """``WAKE`` and ``NOTIFY_COALESCE_SECONDS`` hold values the relay can use."""
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
    """``AROUND_DELIVERY`` is None, or a dotted path to something callable."""
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
    """``RELAY_PRUNE`` is a switch and ``RELAY_PRUNE_SECONDS`` a positive interval."""
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
    """``PRUNE_BATCH_ROWS`` is a positive whole number of rows."""
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
    """No delivery still owed names a receiver the registry no longer has."""
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
    """No event still owed names something the registry cannot decode."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    if not databases:
        return []

    table = EventRecord._meta.db_table
    owed_deliveries = DeliveryRecord.objects.filter(owed(), event=models.OuterRef("pk"))
    names: set[str] = set()
    for alias in databases:
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
