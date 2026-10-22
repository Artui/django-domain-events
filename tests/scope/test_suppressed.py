from __future__ import annotations

import pytest
from django.db import transaction

from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.scope.suppressed import suppressed
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


def test_nested_blocks_accumulate_rather_than_replace(
    order: OrderPlaced, record: list[str]
) -> None:
    """A library suppressing its own event type inside your block must not
    re-enable yours. Replacing the whole suppression is how a suppressed event
    gets recorded as normal and delivered."""
    with suppressed(OrderPlaced, reason="outer"):  # noqa: SIM117 - the nesting is what this asserts
        with suppressed(PinnedName, reason="inner"), transaction.atomic():
            fire(order)
            fire(PinnedName(value=1))

    reasons = dict(EventRecord.objects.values_list("name", "suppressed_reason"))
    assert reasons == {"testapp.OrderPlaced": "outer", "testapp.pinned": "inner"}
    assert record == []


def test_the_innermost_matching_reason_is_the_one_recorded(
    order: OrderPlaced, record: list[str]
) -> None:
    with suppressed(OrderPlaced, reason="outer"):  # noqa: SIM117 - the nesting is what this asserts
        with suppressed(OrderPlaced, reason="inner"), transaction.atomic():
            fire(order)
    assert EventRecord.objects.get().suppressed_reason == "inner"


def test_an_outer_record_false_is_not_undone_by_an_inner_block(
    order: OrderPlaced, record: list[str]
) -> None:
    """The safer half of the disagreement wins: a block that asked for no rows
    should not get them because something nested inside it asked for rows."""
    with suppressed(OrderPlaced, reason="bulk", record=False):  # noqa: SIM117 - the nesting is what this asserts
        with suppressed(OrderPlaced, reason="inner"), transaction.atomic():
            assert fire(order) is None
    assert not EventRecord.objects.exists()


def test_it_refuses_something_that_is_not_a_class() -> None:
    """Passing a name or an instance otherwise fails much later, inside fire(),
    with issubclass complaining about its second argument."""
    with pytest.raises(TypeError, match="event classes"):  # noqa: SIM117 - the nesting is what this asserts
        with suppressed("testapp.OrderPlaced", reason="oops"):
            pass


# Read off the column, as suppressed() reads it, so a widened column moves the
# refusal with it.
_REASON_LIMIT = EventRecord._meta.get_field("suppressed_reason").max_length


def test_a_reason_of_exactly_the_column_width_is_accepted_and_recorded(
    order: OrderPlaced, record: list[str]
) -> None:
    """Holds the comparison: ``>=`` instead of ``>`` refuses this reason. Passes on
    a tree without the check, as it must; it is here for the mutant."""
    assert _REASON_LIMIT == 255
    with transaction.atomic(), suppressed(OrderPlaced, reason="r" * _REASON_LIMIT):
        fire(order)
    assert len(EventRecord.objects.get().suppressed_reason) == _REASON_LIMIT


def test_a_reason_one_over_the_column_width_is_refused_on_entry() -> None:
    reason = "r" * (_REASON_LIMIT + 1)
    with pytest.raises(ValueError) as caught, suppressed(OrderPlaced, reason=reason):
        pytest.fail("the block must not be entered")
    message = str(caught.value)
    assert "reason" in message
    assert f"{_REASON_LIMIT + 1} characters" in message
    assert f"limit is {_REASON_LIMIT}" in message
    # The reason is the caller's, so it is not echoed.
    assert reason[:20] not in message
    # Sized to the width check, not the emptiness one.
    assert "needs a reason" not in message


def test_a_reason_over_the_width_is_refused_even_when_nothing_is_recorded() -> None:
    """``record=False`` writes no row, so the column cannot refuse anything, and
    the refusal holds anyway: whether a block records can be decided by a code
    path the caller's tests never take, and a reason that is fine there would
    fail the first time the other path runs on Postgres. Fails if the check is
    made conditional on ``record``."""
    reason = "r" * (_REASON_LIMIT + 1)
    with (
        pytest.raises(ValueError, match="characters"),
        suppressed(OrderPlaced, reason=reason, record=False),
    ):
        pytest.fail("the block must not be entered")
