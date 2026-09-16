"""Tests mirroring ``django_domain_events/models/delivery_record.py``."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone

import pytest
from django.db import IntegrityError, transaction

from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord

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


def _digest(target: str) -> str:
    return hashlib.sha256(target.encode()).hexdigest()


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
