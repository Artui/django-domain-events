"""Tests mirroring ``django_domain_events/models/delivery_record.py``."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from django.db import IntegrityError, connection, models, transaction

from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_status import DeliveryStatus

pytestmark = pytest.mark.django_db


def _event() -> EventRecord:
    return EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
    )


def test_it_identifies_the_pair_it_represents() -> None:
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    assert str(row) == f"testapp.durable_receiver <- {event.name}#{event.pk} (pending)"


def test_one_delivery_per_event_and_receiver() -> None:
    """The unique constraint is the delivered-log the at-least-once promise owes
    people, rather than a separate table."""
    event = _event()
    DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    with pytest.raises(IntegrityError), transaction.atomic():
        DeliveryRecord.objects.create(
            event=event,
            receiver_key="testapp.durable_receiver",
            available_at=event.recorded_at,
        )


def test_deleting_an_event_takes_its_deliveries() -> None:
    """Cascade, so retention stays a single delete rather than an ordering
    problem."""
    event = _event()
    DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    event.delete()
    assert DeliveryRecord.objects.count() == 0


def test_one_event_may_owe_one_receiver_several_targets() -> None:
    event = _event()
    for target in ("a", "b"):
        DeliveryRecord.objects.create(
            event=event, receiver_key="probe.fan", target=target, available_at=event.recorded_at
        )
    assert DeliveryRecord.objects.filter(event=event).count() == 2


@pytest.mark.parametrize("target", ["a", ""])
def test_two_rows_for_one_event_receiver_and_target_are_refused(target: str) -> None:
    """Including the blank target, which is every receiver without targets=:
    for those this is still the constraint it replaced."""
    event = _event()
    DeliveryRecord.objects.create(
        event=event, receiver_key="probe.fan", target=target, available_at=event.recorded_at
    )
    with pytest.raises(IntegrityError), transaction.atomic():
        DeliveryRecord.objects.create(
            event=event, receiver_key="probe.fan", target=target, available_at=event.recorded_at
        )


def test_a_row_written_without_a_target_has_the_blank_one() -> None:
    """What every row that predates the column holds, and what a receiver
    without targets= writes forever."""
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    row.refresh_from_db()
    assert row.target == ""


def _digest(target: str) -> bytes:
    return hashlib.sha256(target.encode()).digest()


def test_two_rows_with_the_same_long_target_are_refused() -> None:
    """The constraint holds for a target far past any index entry limit."""
    event = _event()
    target = secrets.token_urlsafe(7500)[:10_000]
    DeliveryRecord.objects.create(
        event=event, receiver_key="probe.fan", target=target, available_at=event.recorded_at
    )
    with pytest.raises(IntegrityError), transaction.atomic():
        DeliveryRecord.objects.create(
            event=event, receiver_key="probe.fan", target=target, available_at=event.recorded_at
        )


def test_targets_that_differ_only_past_any_prefix_are_two_deliveries() -> None:
    """Identical for their first 3000 characters - past 255, and past the 2704
    bytes a Postgres index entry holds - and different after. A constraint that
    compared a prefix would refuse the second; the digest covers the whole text."""
    event = _event()
    shared = secrets.token_urlsafe(3000)[:3000]
    for tail in ("-first", "-second"):
        DeliveryRecord.objects.create(
            event=event,
            receiver_key="probe.fan",
            target=shared + tail,
            available_at=event.recorded_at,
        )
    assert DeliveryRecord.objects.filter(event=event).count() == 2


def test_create_writes_the_digest_of_the_target() -> None:
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="probe.fan", target="endpoint-42", available_at=event.recorded_at
    )
    row.refresh_from_db()
    assert row.target_digest == _digest("endpoint-42")


def test_a_row_without_a_target_carries_the_digest_of_the_empty_string() -> None:
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="testapp.durable_receiver", available_at=event.recorded_at
    )
    row.refresh_from_db()
    assert row.target_digest == _digest("")


def test_saving_a_changed_target_rewrites_the_digest() -> None:
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="probe.fan", target="before", available_at=event.recorded_at
    )
    row.target = "after"
    row.save()
    row.refresh_from_db()
    assert row.target_digest == _digest("after")


def test_a_digest_set_by_hand_cannot_disagree_with_the_target() -> None:
    """The case a model default would have let through silently: a write that
    names a target and supplies some other digest, or none. The digest is
    derived at write time, so what is stored is always the target's."""
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event,
        receiver_key="probe.fan",
        target="endpoint-42",
        target_digest=_digest(""),
        available_at=event.recorded_at,
    )
    row.refresh_from_db()
    assert row.target_digest == _digest("endpoint-42")


def test_the_unique_constraint_covers_the_digest_and_no_index_holds_the_text() -> None:
    """Backend-neutral half of the long-target tests.

    Those only go red on Postgres, where an index entry has a size limit; on
    SQLite a constraint on the text itself would pass them. This is what fails
    everywhere if the constraint moves back onto the text.
    """
    [constraint] = DeliveryRecord._meta.constraints
    assert tuple(constraint.fields) == ("event", "receiver_key", "target_digest")
    assert not any("target" in index.fields for index in DeliveryRecord._meta.indexes)
    assert DeliveryRecord._meta.get_field("target").db_index is False


def test_a_stored_digest_is_found_by_the_one_target_digest_computes() -> None:
    """Replay keys a dict by ``target_digest(target)`` and looks the stored
    values up in it, so a stored value has to hash and compare equal to the
    computed one. Asserted through a dict rather than ``==`` because that is
    the operation replay performs; psycopg 2 returns a ``memoryview`` for a
    binary column, which passes both only because a read-only buffer hashes
    as its bytes."""
    event = _event()
    DeliveryRecord.objects.create(
        event=event, receiver_key="probe.fan", target="endpoint-42", available_at=event.recorded_at
    )
    [stored] = DeliveryRecord.objects.values_list("target_digest", flat=True)
    assert {_digest("endpoint-42"): "found"}[stored] == "found"


def test_the_digest_column_is_binary_and_holds_exactly_a_sha256() -> None:
    field = DeliveryRecord._meta.get_field("target_digest")
    assert field.get_internal_type() == "BinaryField"
    assert field.max_length == 32


def test_mysql_gets_a_bounded_binary_column_it_can_index() -> None:
    """MySQL maps a ``BinaryField`` to ``longblob``, which it refuses in a
    unique index without a prefix length. Unverified against a real MySQL
    server: the suite runs on SQLite and Postgres only."""
    field = DeliveryRecord._meta.get_field("target_digest")
    assert field.db_type(SimpleNamespace(vendor="mysql")) == "varbinary(32)"
    assert field.db_type(connection) == connection.data_types["BinaryField"]


def test_a_new_row_has_no_due_time_of_its_own() -> None:
    """NULL is read as the event's ``recorded_at``, which is what lets the
    column arrive on a populated table with nothing to backfill."""
    event = _event()
    row = DeliveryRecord.objects.create(
        event=event, receiver_key="probe.fan", available_at=event.recorded_at
    )
    row.refresh_from_db()
    assert row.due_at is None
    assert DeliveryRecord._meta.get_field("due_at").null is True


def test_no_index_duplicates_the_unique_constraint_on_its_leading_column() -> None:
    """The unique index leads with ``event_id``, which is what the cascade from
    an event and every per-event lookup filter on, so a second index on that
    column alone is written on every insert and read by nothing."""
    [constraint] = DeliveryRecord._meta.constraints
    assert constraint.fields[0] == "event"
    assert DeliveryRecord._meta.get_field("event").db_index is False


def test_the_receiver_key_is_indexed_only_as_the_lead_of_the_last_success_index() -> None:
    """No single-column index and no pattern-ops copy on Postgres: equality and
    ``IN`` on the key are served by the composite, and the admin's search is
    ``icontains``, which neither kind of btree can serve."""
    assert DeliveryRecord._meta.get_field("receiver_key").db_index is False
    [index] = [i for i in DeliveryRecord._meta.indexes if i.name == "dde_last_success"]
    assert tuple(index.fields) == ("receiver_key", "succeeded_at")
    assert index.condition is None


def test_dead_letters_have_a_partial_index_of_their_own() -> None:
    [index] = [i for i in DeliveryRecord._meta.indexes if i.name == "dde_dead_by_receiver"]
    assert tuple(index.fields) == ("receiver_key",)
    assert index.condition == models.Q(status=DeliveryStatus.DEAD)


def test_the_unfinished_rows_of_each_event_have_a_partial_index() -> None:
    """Not the duplicate the unique constraint makes of ``event_id`` alone:
    it holds no succeeded row, so it stays as small as the owed rows and the
    dead letters, and it is what the prune's per-event probes read. Its
    condition is the weaker of the two they ask, so it serves both."""
    [index] = [i for i in DeliveryRecord._meta.indexes if i.name == "dde_unfinished_by_event"]
    assert tuple(index.fields) == ("event",)
    assert index.condition == ~models.Q(status=DeliveryStatus.SUCCEEDED)
