"""Tests mirroring ``django_domain_events/checks.py``."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

import pytest
from django.db import transaction

from django_domain_events import checks
from django_domain_events.fire import fire
from django_domain_events.registry import registry
from django_domain_events.suppressed import suppressed
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import event_deleted, receiver_deleted
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
    from django_domain_events.drain_outbox import drain_outbox

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
