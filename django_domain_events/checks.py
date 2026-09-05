from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from django.conf import settings
from django.core.checks import Error, Warning
from django.db import models
from django.utils.module_loading import import_string

from django_domain_events.registry import registry
from django_domain_events.settings import DEFAULTS, SETTINGS_NAME, get_codec, setting
from django_domain_events.utils import TERMINAL, has_table

# The names a reader reaches for instead: the app label, and the prose everyone
# writes. Neither is read, and neither fails.
_NEAR_MISS_SETTINGS_NAMES = ("DOMAIN_EVENTS", "DJANGO_DOMAIN_EVENT", "DOMAIN_EVENT")


def check_receivers_have_events(**kwargs: Any) -> list[Any]:
    """Every receiver listens for something that was declared.

    Nothing else would say so: the event cannot be fired, so there is no failure
    to observe, only silence.
    """
    problems = []
    for receiver in registry.receivers():
        if registry.event_for_class(receiver.event_class) is None:
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
    """The settings dict is named correctly and holds no unrecognised keys.

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
    return problems


def check_no_orphaned_deliveries(
    *, databases: Sequence[str] | None = None, **kwargs: Any
) -> list[Any]:
    """No delivery still owed names a receiver the registry no longer has.

    Owed means "not terminal", the same definition the relay claims by and the
    prune settles by. Listing the owed statuses instead is how this check came
    to omit CLAIMED: a worker that died between claiming a row and the deploy
    that deleted its receiver leaves the row claimed with a lapsed lease, and it
    read as settled until a relay happened to reclaim it.

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
            .exclude(status__in=TERMINAL)
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
    owed = DeliveryRecord.objects.filter(event=models.OuterRef("pk")).exclude(status__in=TERMINAL)
    names: set[str] = set()
    for alias in databases:
        # One table answers for both: they are created by the same
        # migration, so neither exists without the other.
        if not has_table(alias, table):
            continue
        names |= set(
            EventRecord.objects.using(alias)
            .filter(models.Exists(owed))
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
