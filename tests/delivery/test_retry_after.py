from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime, timedelta
from unittest import mock

import pytest
from django.db import transaction
from django.utils import timezone as django_timezone

import django_domain_events
from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery.backoff import backoff
from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    """Take this file's ad-hoc receivers back out of the process-wide registry."""
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _now() -> datetime:
    return django_timezone.now()


def _fire() -> None:
    with transaction.atomic():
        fire(Unheard(value=1))


def _asking_for(seconds: float, key: str, **declaration: object) -> None:
    def rate_limited(event: Unheard) -> None:
        raise RetryAfter(seconds=seconds, reason=f"the endpoint sent Retry-After: {seconds:g}")

    receiver(Unheard, key=key, **declaration)(rate_limited)


def _other_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The warnings a test is about, without the line every failed attempt logs."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and " failed delivery " not in r.getMessage()
    ]


def test_it_is_exported_from_the_package_root() -> None:
    assert django_domain_events.RetryAfter is RetryAfter


def test_the_message_is_the_reason() -> None:
    assert str(RetryAfter(120, reason="the endpoint sent Retry-After: 120")) == (
        "the endpoint sent Retry-After: 120"
    )


def test_with_no_reason_the_message_still_says_what_was_asked() -> None:
    assert str(RetryAfter(120)) == "retry requested in 120s"
    assert RetryAfter(1.5).seconds == 1.5


@pytest.mark.parametrize("seconds", [-1, -0.001, float("nan")])
def test_a_delay_that_is_not_a_time_from_now_is_refused(seconds: float) -> None:
    """NaN is in the list because it passes a plain ``< 0`` check."""
    with pytest.raises(ValueError, match="zero seconds or more"):
        RetryAfter(seconds)


def test_zero_means_as_soon_as_possible() -> None:
    assert RetryAfter(0).seconds == 0.0


@pytest.mark.parametrize("seconds", [0, 0.0])
def test_a_deferral_that_does_not_count_must_ask_for_a_wait(seconds: float) -> None:
    """Held by the ``not seconds > 0`` conjunct of the ``counts=False`` guard."""
    with pytest.raises(ValueError, match="above zero seconds"):
        RetryAfter(seconds, counts=False)
    assert RetryAfter(seconds).seconds == 0.0


def test_a_nan_deferral_that_does_not_count_is_refused_by_the_plain_check() -> None:
    with pytest.raises(ValueError, match="zero seconds or more"):
        RetryAfter(float("nan"), counts=False)


def test_it_sets_the_schedule_instead_of_the_backoff_curve(settings) -> None:
    """The requested delay, not the curve."""
    settings.DJANGO_DOMAIN_EVENTS = {"BACKOFF_BASE_SECONDS": 1000.0, "BACKOFF_CAP_SECONDS": 1000.0}
    assert backoff(1, base=1000.0, cap=1000.0, jitter=0.5) == timedelta(seconds=500)

    _asking_for(120, "probe.limited")
    _fire()

    before = _now()
    with mock.patch("django_domain_events.delivery.deliver.random.random", return_value=0.5):
        assert deliver_pending(limit=1) == {DeliveryStatus.FAILED: 1}
    after = _now()

    row = DeliveryRecord.objects.get(receiver_key="probe.limited")
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 1)
    assert before + timedelta(seconds=120) <= row.available_at <= after + timedelta(seconds=120)
    assert row.last_error == "RetryAfter: the endpoint sent Retry-After: 120"


def test_it_consumes_an_attempt() -> None:
    """A destination answering 429 forever still dead-letters."""
    seen: list[DeliveryFailure] = []
    _asking_for(0, "probe.forever", max_attempts=2, on_failure=seen.append)
    _fire()

    assert deliver_pending(limit=1) == {DeliveryStatus.FAILED: 1}
    assert deliver_pending(limit=1) == {DeliveryStatus.DEAD: 1}

    row = DeliveryRecord.objects.get(receiver_key="probe.forever")
    assert (row.status, row.attempts) == (DeliveryStatus.DEAD, 2)
    assert [(failure.status, failure.attempt) for failure in seen] == [
        (DeliveryStatus.FAILED, 1),
        (DeliveryStatus.DEAD, 2),
    ]


def test_a_delay_past_the_ceiling_is_clamped_and_warned_about(
    settings, caplog: pytest.LogCaptureFixture
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"MAX_RECEIVER_RETRY_DELAY_SECONDS": 300.0}
    _asking_for(3600, "probe.patient")
    _fire()

    before = _now()
    with caplog.at_level(logging.WARNING, logger="django_domain_events.delivery.deliver"):
        assert deliver_pending(limit=1) == {DeliveryStatus.FAILED: 1}
    after = _now()

    row = DeliveryRecord.objects.get(receiver_key="probe.patient")
    assert before + timedelta(seconds=300) <= row.available_at <= after + timedelta(seconds=300)

    warnings = _other_warnings(caplog)
    assert len(warnings) == 1
    # Both numbers, and whose request it was: ceiling or destination to question.
    assert "probe.patient" in warnings[0]
    assert "3600.0s" in warnings[0]
    assert "MAX_RECEIVER_RETRY_DELAY_SECONDS of 300.0s" in warnings[0]


def test_a_delay_at_the_ceiling_is_not_clamped(settings, caplog: pytest.LogCaptureFixture) -> None:
    """The boundary belongs to the receiver: exactly the ceiling is allowed."""
    settings.DJANGO_DOMAIN_EVENTS = {"MAX_RECEIVER_RETRY_DELAY_SECONDS": 300.0}
    _asking_for(300, "probe.exact")
    _fire()

    with caplog.at_level(logging.WARNING, logger="django_domain_events.delivery.deliver"):
        deliver_pending(limit=1)

    assert not _other_warnings(caplog)


# --- A deferral that spends no attempt ---------------------------------------


class _Runaway(BaseException):
    """Raised by a receiver called more often than a working deferral allows,
    so a regression fails the test instead of hanging the suite. A
    BaseException because delivery records every ``Exception`` as a failure."""


def _deferring(key: str, seconds: float = 60.0, **declaration: object) -> list[int]:
    """Declare a receiver that defers without counting, returning the attempt seen."""
    seen: list[int] = []

    def throttled(event: Unheard, ctx: DeliveryContext) -> None:
        seen.append(ctx.attempt)
        if len(seen) > 5:
            raise _Runaway("a deferred row kept being claimed")
        raise RetryAfter(seconds, reason="the provider is throttling", counts=False)

    receiver(Unheard, key=key, takes_context=True, **declaration)(throttled)
    return seen


def test_counting_is_the_default() -> None:
    """Every caller from before the argument existed keeps spending an attempt."""
    assert RetryAfter(1).counts is True
    assert RetryAfter(1, counts=False).counts is False


def test_a_deferral_that_does_not_count_leaves_the_attempts_and_the_budget_alone() -> None:
    """A budget of one shows whether it was spent: a counting deferral dead-letters."""
    seen = _deferring("probe.throttled", max_attempts=1, give_up_after=timedelta(days=1))
    _fire()

    assert deliver_pending(limit=1, ignore_backoff=True) == {DeliveryStatus.FAILED: 1}
    assert deliver_pending(limit=1, ignore_backoff=True) == {DeliveryStatus.FAILED: 1}

    row = DeliveryRecord.objects.get(receiver_key="probe.throttled")
    assert (row.status, row.attempts) == (DeliveryStatus.FAILED, 0)
    assert row.completed_at is None
    assert row.last_error == "RetryAfter: the provider is throttling"
    assert seen == [1, 1]


@pytest.mark.django_db(transaction=True)
def test_an_eager_deferral_is_recorded_unspent_and_pauses_nothing() -> None:
    """The eager attempt has no lane to pause, so each fire calls the destination again."""
    seen = _deferring(
        "probe.eager_throttled", eager=True, max_attempts=1, give_up_after=timedelta(days=1)
    )
    _fire()
    _fire()

    rows = DeliveryRecord.objects.filter(receiver_key="probe.eager_throttled")
    assert [(row.status, row.attempts) for row in rows] == [(DeliveryStatus.FAILED, 0)] * 2
    assert seen == [1, 1]


def test_a_counting_retry_after_still_spends_its_attempt() -> None:
    """The same budget of one, counted: the row dead-letters on the first request."""
    _asking_for(0, "probe.counted", max_attempts=1, give_up_after=timedelta(days=1))
    _fire()

    assert deliver_pending(limit=1) == {DeliveryStatus.DEAD: 1}
    row = DeliveryRecord.objects.get(receiver_key="probe.counted")
    assert (row.status, row.attempts) == (DeliveryStatus.DEAD, 1)


def test_an_ordinary_exception_is_not_read_as_a_deferral() -> None:
    """An exception without counts is an ordinary failure, not an AttributeError."""

    def broken(event: Unheard) -> None:
        raise RuntimeError("down")

    receiver(Unheard, key="probe.broken", give_up_after=timedelta(days=1))(broken)
    _fire()

    assert deliver_pending(limit=1) == {DeliveryStatus.FAILED: 1}
    assert DeliveryRecord.objects.get(receiver_key="probe.broken").attempts == 1


@pytest.mark.parametrize(("jitter", "expected"), [(0.0, 60), (0.5, 90), (0.999, 119.94)])
def test_the_requested_delay_is_jittered_upwards_only(jitter: float, expected: float) -> None:
    """Never earlier than asked, and spread over as long again."""
    _deferring("probe.spread", seconds=60, give_up_after=timedelta(days=1))
    _fire()

    before = _now()
    with mock.patch("django_domain_events.delivery.deliver.random.random", return_value=jitter):
        deliver_pending(limit=1)
    after = _now()

    row = DeliveryRecord.objects.get(receiver_key="probe.spread")
    wait = timedelta(seconds=expected)
    assert before + wait <= row.available_at <= after + wait


def test_a_jittered_delay_stays_under_the_ceiling(
    settings, caplog: pytest.LogCaptureFixture
) -> None:
    """The ceiling bounds where a row can park, jitter included."""
    settings.DJANGO_DOMAIN_EVENTS = {"MAX_RECEIVER_RETRY_DELAY_SECONDS": 100.0}
    _deferring("probe.near_ceiling", seconds=80, give_up_after=timedelta(days=1))
    _fire()

    before = _now()
    with (
        mock.patch("django_domain_events.delivery.deliver.random.random", return_value=0.9),
        caplog.at_level(logging.WARNING, logger="django_domain_events.delivery.deliver"),
    ):
        deliver_pending(limit=1)
    after = _now()

    row = DeliveryRecord.objects.get(receiver_key="probe.near_ceiling")
    ceiling = timedelta(seconds=100)
    assert before + ceiling <= row.available_at <= after + ceiling
    assert not _other_warnings(caplog)


def test_on_failure_hears_a_deferral_with_the_attempt_the_receiver_was_told() -> None:
    seen: list[DeliveryFailure] = []
    _deferring("probe.heard", seconds=1, give_up_after=timedelta(days=1), on_failure=seen.append)
    _fire()

    deliver_pending(limit=1, ignore_backoff=True)
    deliver_pending(limit=1, ignore_backoff=True)

    assert [(f.status, f.attempt) for f in seen] == [
        (DeliveryStatus.FAILED, 1),
        (DeliveryStatus.FAILED, 1),
    ]


def _owed_since(key: str, *, recorded: timedelta, due: timedelta | None) -> None:
    """Back-date the event behind ``key``'s row, and its ``due_at`` if given."""
    row = DeliveryRecord.objects.select_related("event").get(receiver_key=key)
    EventRecord.objects.filter(pk=row.event.pk).update(recorded_at=_now() - recorded)
    DeliveryRecord.objects.filter(pk=row.pk).update(due_at=None if due is None else _now() - due)


def test_a_deferral_past_give_up_after_dead_letters_the_row() -> None:
    """Measured from recorded_at when due_at is NULL, the fallback."""
    seen: list[DeliveryFailure] = []
    _deferring("probe.expired", give_up_after=timedelta(days=1), on_failure=seen.append)
    _fire()
    _owed_since("probe.expired", recorded=timedelta(days=2), due=None)

    assert deliver_pending(limit=1) == {DeliveryStatus.DEAD: 1}

    row = DeliveryRecord.objects.get(receiver_key="probe.expired")
    assert (row.status, row.attempts) == (DeliveryStatus.DEAD, 0)
    assert row.completed_at is not None
    assert "give_up_after=1 day" in row.last_error
    assert "the provider is throttling" in row.last_error
    assert [(f.status, f.attempt) for f in seen] == [(DeliveryStatus.DEAD, 1)]


def test_a_deferral_inside_give_up_after_is_still_deferred() -> None:
    _deferring("probe.young", give_up_after=timedelta(days=1))
    _fire()
    _owed_since("probe.young", recorded=timedelta(hours=23), due=None)

    assert deliver_pending(limit=1) == {DeliveryStatus.FAILED: 1}


def test_due_at_is_read_before_the_event_timestamp() -> None:
    """A reopened row is measured from the reopening, not the event's timestamp."""
    _deferring("probe.reopened", give_up_after=timedelta(days=1), lane="a")
    _deferring("probe.overdue", give_up_after=timedelta(days=1), lane="b")
    _fire()
    _owed_since("probe.reopened", recorded=timedelta(days=7), due=timedelta(hours=1))
    _owed_since("probe.overdue", recorded=timedelta(hours=1), due=timedelta(days=2))

    deliver_pending()

    statuses = dict(
        DeliveryRecord.objects.filter(receiver_key__startswith="probe.").values_list(
            "receiver_key", "status"
        )
    )
    assert statuses == {"probe.reopened": DeliveryStatus.FAILED, "probe.overdue": "dead"}


@pytest.mark.parametrize(
    ("elapsed", "given_up"),
    [
        (timedelta(days=1), True),
        (timedelta(days=1, microseconds=1), True),
        (timedelta(days=1) - timedelta(microseconds=1), False),
    ],
)
def test_the_bound_is_reached_at_exactly_give_up_after(elapsed: timedelta, given_up: bool) -> None:
    """Both sides of the comparison, which a real clock cannot pin."""
    now = _now()
    assert deliver_module._given_up(now - elapsed, now, timedelta(days=1)) is given_up


def test_neither_a_deferral_nor_a_counted_failure_moves_due_at() -> None:
    """``due_at`` is when the row became owed."""
    _deferring("probe.still_due", give_up_after=timedelta(days=1), lane="slow")
    _asking_for(60, "probe.still_due_counted")
    _fire()

    assert deliver_pending() == {DeliveryStatus.FAILED: 2}

    assert list(
        DeliveryRecord.objects.filter(receiver_key__startswith="probe.still_due").values_list(
            "due_at", flat=True
        )
    ) == [None, None]


def test_a_deferral_with_no_give_up_after_counts_and_says_so_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Nothing else ends it, so it is spent like any RetryAfter, named once per process."""
    _deferring("probe.unbounded", seconds=1, max_attempts=3)
    _fire()
    _fire()

    with caplog.at_level(logging.WARNING, logger="django_domain_events.delivery.deliver"):
        deliver_pending(ignore_backoff=True, limit=2)
        deliver_pending(ignore_backoff=True, limit=2)

    attempts = list(
        DeliveryRecord.objects.filter(receiver_key="probe.unbounded").values_list(
            "attempts", flat=True
        )
    )
    assert attempts == [2, 2]
    warnings = _other_warnings(caplog)
    assert len(warnings) == 1
    assert "probe.unbounded" in warnings[0]
    assert "give_up_after" in warnings[0]


def test_a_drain_meeting_a_deferral_ends() -> None:
    """A non-counting deferral leaves nothing else to end drain_outbox's loop."""
    seen = _deferring("probe.drained", seconds=1, give_up_after=timedelta(days=1))
    _fire()

    assert drain_outbox() == {DeliveryStatus.FAILED: 1}
    assert seen == [1]
