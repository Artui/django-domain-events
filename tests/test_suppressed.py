from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.suppressed import suppressed
from tests.testapp.events import OrderPlaced, PinnedName

pytestmark = pytest.mark.django_db(transaction=True)


def test_a_suppressed_event_is_recorded_with_its_reason(
    order: OrderPlaced, record: list[str]
) -> None:
    """The default writes the row and marks it. A silently dropped event is
    unauditable, which is the failure mode suppression is most likely to cause."""
    with suppressed(OrderPlaced, reason="historical import"), transaction.atomic():
        fire(order)

    row = EventRecord.objects.get()
    assert row.suppressed_reason == "historical import"
    assert not DeliveryRecord.objects.exists()


def test_no_receiver_runs_at_any_execution_site(order: OrderPlaced, record: list[str]) -> None:
    """Suppression is about the event, not about one execution site: inline and
    on-commit receivers are skipped too."""
    with suppressed(OrderPlaced, reason="import"), transaction.atomic():
        fire(order)
    assert record == []


def test_record_false_discards_without_a_row(order: OrderPlaced, record: list[str]) -> None:
    """The escape hatch for a bulk import, where writing a row per suppressed
    event is the surprise. Named rather than defaulted, because it trades away
    the audit trail the default exists for."""
    with suppressed(OrderPlaced, reason="bulk", record=False), transaction.atomic():
        assert fire(order) is None
    assert not EventRecord.objects.exists()


def test_other_events_are_untouched(order: OrderPlaced, record: list[str]) -> None:
    with suppressed(PinnedName, reason="import"), transaction.atomic():
        fire(order)
    row = EventRecord.objects.get()
    assert row.suppressed_reason == ""
    assert DeliveryRecord.objects.exists()


def test_suppressing_everything_takes_no_classes(order: OrderPlaced, record: list[str]) -> None:
    with suppressed(reason="maintenance"), transaction.atomic():
        fire(order)
        fire(PinnedName(value=1))
    assert EventRecord.objects.filter(suppressed_reason="maintenance").count() == 2


def test_a_reason_is_required() -> None:
    """An event dropped without one is indistinguishable from a bug."""
    with pytest.raises(ValueError, match="needs a reason"), suppressed(OrderPlaced, reason=""):
        pass


def test_suppression_does_not_leak_past_the_block(order: OrderPlaced, record: list[str]) -> None:
    with suppressed(OrderPlaced, reason="import"):
        pass
    with transaction.atomic():
        fire(order)
    assert EventRecord.objects.get().suppressed_reason == ""
