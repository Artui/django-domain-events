"""Tests mirroring ``django_domain_events/checks.py``."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from django.db import connection, transaction

from django_domain_events import checks
from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.fire import fire
from django_domain_events.scope.suppressed import suppressed
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import Plans, delivery_table_access, event_deleted, receiver_deleted
from tests.testapp.events import OrderPlaced


@dataclass(frozen=True)
class NeverDeclared:
    value: int


@contextmanager
def _receiver_registered(key: str, event_class: type):
    entry = RegisteredReceiver(
        key=key,
        event_class=event_class,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
    )
    registry.register_receiver(entry)
    try:
        yield
    finally:
        registry._receivers.pop(key, None)


def test_a_receiver_for_an_undeclared_event_is_an_error() -> None:
    """Nothing else would ever say so: the event cannot be fired, so there is no
    failure to observe, only silence."""
    with _receiver_registered("testapp.dangling", NeverDeclared):
        problems = checks.check_receivers_have_events()
    assert [p.id for p in problems] == ["django_domain_events.E001"]
    assert "NeverDeclared" in problems[0].msg


def test_receivers_all_matched_is_clean() -> None:
    assert checks.check_receivers_have_events() == []


def test_an_importable_codec_is_clean() -> None:
    assert checks.check_codec_dependency_is_installed() == []


def test_an_unimportable_codec_is_reported_at_startup(settings) -> None:
    """A codec is imported lazily, so without this the first symptom of a
    missing extra is a delivery failing in a worker."""
    settings.DJANGO_DOMAIN_EVENTS = {"CODEC": "django_domain_events.codecs.nope.NoSuchCodec"}
    problems = checks.check_codec_dependency_is_installed()
    assert [p.id for p in problems] == ["django_domain_events.E002"]
    assert "dacite" in problems[0].hint


@pytest.mark.django_db(transaction=True)
def test_pending_rows_for_a_deleted_receiver_are_reported(
    order: OrderPlaced, record: list[str]
) -> None:
    """The cost of freezing the receiver set at fire time, surfaced as a question
    rather than found in a log."""
    with transaction.atomic():
        fire(order)

    with receiver_deleted("testapp.durable_receiver"):
        problems = checks.check_no_orphaned_deliveries(databases=["default"])

    assert [p.id for p in problems] == ["django_domain_events.W001"]
    assert "testapp.durable_receiver" in problems[0].msg


@pytest.mark.django_db(transaction=True)
def test_no_pending_rows_is_clean() -> None:
    assert checks.check_no_orphaned_deliveries(databases=["default"]) == []


def test_it_does_nothing_without_a_database_to_look_at() -> None:
    """``check``, ``showmigrations`` and ``makemigrations`` pass no databases.
    Querying anyway is how a check registered under the database tag ends up
    running where there is no database to run against."""
    assert checks.check_no_orphaned_deliveries() == []
    assert checks.check_no_orphaned_deliveries(databases=[]) == []


@pytest.mark.django_db(transaction=True)
def test_it_does_nothing_before_the_table_exists() -> None:
    """The one that made the package uninstallable: ``migrate`` does pass a
    database, and runs this before creating the tables. Querying there kills the
    first command a new project runs, and no tables are created at all.
    """
    from django.db import connection

    from django_domain_events.models.delivery_record import DeliveryRecord

    with connection.schema_editor() as editor:
        editor.delete_model(DeliveryRecord)
    try:
        assert checks.check_no_orphaned_deliveries(databases=["default"]) == []
    finally:
        with connection.schema_editor() as editor:
            editor.create_model(DeliveryRecord)


@pytest.mark.django_db(transaction=True)
def test_a_renamed_event_with_work_owed_is_reported(order: OrderPlaced, record: list[str]) -> None:
    """The case the orphan warning cannot see: the receivers keep their keys,
    so nothing looks orphaned, while every row written under the old name now
    decodes to nothing and spends one attempt budget at a time finding out."""
    with transaction.atomic():
        fire(order)

    with event_deleted("testapp.OrderPlaced"):
        problems = checks.check_recorded_events_are_declared(databases=["default"])

    assert [p.id for p in problems] == ["django_domain_events.W002"]
    assert "testapp.OrderPlaced" in problems[0].msg
    assert "@event(name=" in problems[0].hint


@pytest.mark.django_db(transaction=True)
def test_a_declared_event_is_clean(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    assert checks.check_recorded_events_are_declared(databases=["default"]) == []


@pytest.mark.django_db(transaction=True)
def test_a_settled_row_naming_a_retired_event_is_not_reported(
    order: OrderPlaced, record: list[str]
) -> None:
    """History, not a problem. Warning about it on every ``check`` run teaches
    the reader to skip the output."""
    from django_domain_events.delivery.drain_outbox import drain_outbox

    with transaction.atomic():
        fire(order)
    drain_outbox()

    with event_deleted("testapp.OrderPlaced"):
        assert checks.check_recorded_events_are_declared(databases=["default"]) == []


@pytest.mark.django_db(transaction=True)
def test_an_event_with_no_deliveries_at_all_is_not_reported(
    order: OrderPlaced, record: list[str]
) -> None:
    """A suppressed row is owed to nobody, so nothing can dead-letter."""
    with transaction.atomic(), suppressed(OrderPlaced, reason="test"):
        fire(order)

    with event_deleted("testapp.OrderPlaced"):
        assert checks.check_recorded_events_are_declared(databases=["default"]) == []


def test_the_declaration_check_does_nothing_without_a_database() -> None:
    assert checks.check_recorded_events_are_declared() == []
    assert checks.check_recorded_events_are_declared(databases=[]) == []


@pytest.mark.django_db(transaction=True)
def test_the_declaration_check_does_nothing_before_the_table_exists() -> None:
    """Registered under the database tag, so ``migrate`` runs it before the
    tables it queries exist."""
    from django.db import connection

    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord

    with connection.schema_editor() as editor:
        editor.delete_model(DeliveryRecord)
        editor.delete_model(EventRecord)
    try:
        assert checks.check_recorded_events_are_declared(databases=["default"]) == []
    finally:
        with connection.schema_editor() as editor:
            editor.create_model(EventRecord)
            editor.create_model(DeliveryRecord)


@pytest.mark.django_db(transaction=True)
def test_a_claimed_row_for_a_deleted_receiver_is_still_owed(
    order: OrderPlaced, record: list[str]
) -> None:
    """A worker that died between claiming a row and the deploy that deleted
    its receiver leaves the row claimed with a lapsed lease. Listing the owed
    statuses instead of excluding the terminal ones read that as settled, so
    ``check`` called the log clean until a relay happened to reclaim it."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.types.delivery_status import DeliveryStatus

    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.filter(receiver_key="testapp.durable_receiver").update(
        status=DeliveryStatus.CLAIMED, claimed_by="dead-worker"
    )

    with receiver_deleted("testapp.durable_receiver"):
        problems = checks.check_no_orphaned_deliveries(databases=["default"])

    assert [p.id for p in problems] == ["django_domain_events.W001"]
    assert "testapp.durable_receiver" in problems[0].msg


@pytest.mark.django_db(transaction=True)
def test_both_warnings_agree_on_what_is_still_owed(order: OrderPlaced, record: list[str]) -> None:
    """The docs say they do, and they did not: one omitted CLAIMED."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.types.delivery_status import DeliveryStatus

    with transaction.atomic():
        fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.CLAIMED, claimed_by="dead-worker")

    with receiver_deleted("testapp.durable_receiver"), event_deleted("testapp.OrderPlaced"):
        orphaned = checks.check_no_orphaned_deliveries(databases=["default"])
        undeclared = checks.check_recorded_events_are_declared(databases=["default"])
    assert [p.id for p in orphaned] == ["django_domain_events.W001"]
    assert [p.id for p in undeclared] == ["django_domain_events.W002"]


def test_a_wildcard_receiver_is_not_an_undeclared_event() -> None:
    """AnyEvent is a marker, never declared as an event, and a receiver for it
    listens to everything that is."""
    with _receiver_registered("testapp.everything", AnyEvent):
        assert checks.check_receivers_have_events() == []


def test_the_default_wake_settings_are_clean() -> None:
    assert checks.check_settings_keys_are_known() == []


@pytest.mark.parametrize("mode", ["notify", "poll"])
def test_a_known_wake_mode_is_clean(settings, mode: str) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"WAKE": mode}
    assert checks.check_settings_keys_are_known() == []


@pytest.mark.parametrize("mode", ["Notify", "listen", "", None])
def test_a_misspelt_wake_mode_is_an_error(settings, mode: object) -> None:
    """Anything but ``notify`` would otherwise quietly turn NOTIFY off."""
    settings.DJANGO_DOMAIN_EVENTS = {"WAKE": mode}
    problems = checks.check_settings_keys_are_known()
    assert [p.id for p in problems] == ["django_domain_events.E006"]
    assert repr(mode) in problems[0].msg
    assert "'notify', 'poll'" in problems[0].hint


@pytest.mark.parametrize("interval", [0, 0.0, 0.5, 2])
def test_a_coalesce_interval_of_zero_or_more_is_clean(settings, interval: float) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"NOTIFY_COALESCE_SECONDS": interval}
    assert checks.check_settings_keys_are_known() == []


@pytest.mark.parametrize("interval", [-1, float("nan"), "0.5", None, True])
def test_a_coalesce_interval_that_is_not_a_duration_is_an_error(settings, interval: object) -> None:
    """One case per refusal: a negative, NaN (which compares false both ways), a
    string, and a bool (an ``int``, so it would read as one second)."""
    settings.DJANGO_DOMAIN_EVENTS = {"NOTIFY_COALESCE_SECONDS": interval}
    problems = checks.check_settings_keys_are_known()
    assert [p.id for p in problems] == ["django_domain_events.E007"]


@pytest.mark.parametrize("interval", [1, 0.5, 60, 3600.0])
def test_a_positive_prune_interval_is_clean(settings, interval: float) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": interval}
    assert checks.check_settings_keys_are_known() == []


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf"), "60", None, True])
def test_a_prune_interval_that_is_not_a_positive_duration_is_an_error(
    settings, interval: object
) -> None:
    """One case per refusal: zero and a negative, NaN (false against everything),
    infinity, a string, ``None`` and a bool (an ``int``, so it would read as one second)."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE_SECONDS": interval}
    problems = checks.check_settings_keys_are_known()
    assert [p.id for p in problems] == ["django_domain_events.E009"]
    assert repr(interval) in problems[0].msg


@pytest.mark.parametrize("size", [1, 500, 5000])
def test_a_positive_prune_batch_is_clean(settings, size: int) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"PRUNE_BATCH_ROWS": size}
    assert checks.check_settings_keys_are_known() == []


@pytest.mark.parametrize("size", [0, -5, 2.5, "5000", None, True])
def test_a_prune_batch_that_is_not_a_positive_count_is_an_error(settings, size: object) -> None:
    """The bool test by ``True`` (an ``int``, so a batch of one), the type test
    by ``2.5``, ``"5000"`` and ``None``, the sign test by ``0`` and ``-5``."""
    settings.DJANGO_DOMAIN_EVENTS = {"PRUNE_BATCH_ROWS": size}
    problems = checks.check_settings_keys_are_known()
    assert [p.id for p in problems] == ["django_domain_events.E010"]
    assert repr(size) in problems[0].msg


@pytest.mark.parametrize("switch", [True, False])
def test_the_prune_switch_is_clean_as_a_bool(settings, switch: bool) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE": switch}
    assert checks.check_settings_keys_are_known() == []


@pytest.mark.parametrize("switch", ["false", 0, 1, None])
def test_a_prune_switch_that_is_not_a_bool_is_an_error(settings, switch: object) -> None:
    """``"false"`` is the case the check exists for: truthy, so the operator who
    meant the sweep off would have it on."""
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_PRUNE": switch}
    problems = checks.check_settings_keys_are_known()
    assert [p.id for p in problems] == ["django_domain_events.E008"]
    assert repr(switch) in problems[0].msg


def _one_row_in_every_status(name_for: Callable[[str], str], key_for: Callable[[str], str]) -> None:
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.event_record import EventRecord
    from django_domain_events.types.delivery_status import DeliveryStatus

    for status in DeliveryStatus:
        event = EventRecord.objects.create(
            name=name_for(status.value),
            version=1,
            payload={},
            occurred_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
        )
        DeliveryRecord.objects.create(
            event=event,
            receiver_key=key_for(status.value),
            status=status,
            available_at=event.recorded_at,
        )


def _owed_statuses() -> list[str]:
    from django_domain_events.types.delivery_status import DeliveryStatus
    from django_domain_events.utils import TERMINAL

    return sorted(s.value for s in DeliveryStatus if s not in TERMINAL)


@pytest.mark.django_db
def test_the_orphan_warning_counts_every_owed_status_and_no_other() -> None:
    """Owed is phrased as the partial indexes' conditions, which lists the owed
    statuses; ``exclude(status__in=TERMINAL)`` could not miss one. One orphaned
    row in every status there is, so a status added later and left out of the
    phrasing - or CLAIMED dropped again - turns this red."""
    _one_row_in_every_status(lambda s: "testapp.OrderPlaced", lambda s: f"gone.{s}")

    [problem] = checks.check_no_orphaned_deliveries(databases=["default"])

    assert problem.msg.endswith(": " + ", ".join(f"gone.{s}" for s in _owed_statuses()) + ".")


@pytest.mark.django_db
def test_the_undeclared_event_warning_counts_every_owed_status_and_no_other() -> None:
    _one_row_in_every_status(lambda s: f"gone.{s}", lambda s: "testapp.durable_receiver")

    [problem] = checks.check_recorded_events_are_declared(databases=["default"])

    assert problem.msg.endswith(": " + ", ".join(f"gone.{s}" for s in _owed_statuses()) + ".")


postgres_only = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="asserts Postgres query plans; SQLite's planner is not the one migrate runs on",
)
OWED_INDEXES = {"dde_owed_by_available_at", "dde_claimed_by_lease"}


@postgres_only
@pytest.mark.django_db
def test_the_orphan_check_reads_only_the_owed_partial_indexes(
    plans_without_seqscan: Plans,
) -> None:
    """It runs on every ``migrate`` and ``check``. ``status NOT IN (terminal)``
    matches neither partial index, so it walked every delivery ever made."""
    plans = plans_without_seqscan(
        lambda: checks.check_no_orphaned_deliveries(databases=["default"])
    )
    reads = [delivery_table_access(p) for p in plans if "deliveryrecord" in p]
    assert reads == [OWED_INDEXES], plans


@postgres_only
@pytest.mark.django_db
def test_the_undeclared_event_check_reads_only_the_owed_partial_indexes(
    plans_without_seqscan: Plans,
) -> None:
    plans = plans_without_seqscan(
        lambda: checks.check_recorded_events_are_declared(databases=["default"])
    )
    reads = [delivery_table_access(p) for p in plans if "deliveryrecord" in p]
    assert reads == [OWED_INDEXES], plans
