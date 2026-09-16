"""Tests mirroring ``django_domain_events/delivery/retry_after.py``.

Construction is tested directly; everything else is what the relay does on
reading it, because that is the only place the delay means anything.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from django.db import transaction

import django_domain_events
from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.backoff import backoff
from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.models.delivery_record import DeliveryRecord
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
    return datetime.now(timezone.utc)


def _fire() -> None:
    with transaction.atomic():
        fire(Unheard(value=1))


def _asking_for(seconds: float, key: str, **declaration: object) -> None:
    def rate_limited(event: Unheard) -> None:
        raise RetryAfter(seconds=seconds, reason=f"the endpoint sent Retry-After: {seconds:g}")

    receiver(Unheard, key=key, **declaration)(rate_limited)


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


def test_it_sets_the_schedule_instead_of_the_backoff_curve(settings) -> None:
    """The requested delay, not the curve.

    The curve is configured to land somewhere visibly different - 500 seconds
    with the jitter pinned, against 120 asked for - so the assertion cannot pass
    because the two happened to agree.
    """
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
    """A destination answering 429 forever still dead-letters.

    Asking for zero seconds so the second attempt is claimable at once; the
    budget is two, so the second request is the last one there is.
    """
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

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    # Both numbers, and whose request it was, so an operator can tell whether
    # the ceiling or the destination is the thing to question.
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

    assert not [r for r in caplog.records if r.levelno == logging.WARNING]
