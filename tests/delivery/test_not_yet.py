"""Tests mirroring ``django_domain_events/delivery/not_yet.py``.

Construction is tested directly; everything else is what the relay does on
reading it, because that is the only place the time means anything.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import pytest
from django.db import transaction
from django.utils import timezone as django_timezone

import django_domain_events
from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_one, deliver_pending
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.not_yet import NotYet
from django_domain_events.introspection.outbox_health import outbox_health
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db

KEY = "probe.expire_offer"


class _Runaway(BaseException):
    """Escapes the relay's ``except Exception``, ending a pass that would loop."""


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    """Take this file's ad-hoc receivers back out of the process-wide registry."""
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _now() -> datetime:
    return django_timezone.now()


def _fire(value: int = 1) -> None:
    with transaction.atomic():
        fire(Unheard(value=value))


def _row(key: str = KEY) -> DeliveryRecord:
    return DeliveryRecord.objects.get(receiver_key=key)


def _extended_to(until: datetime, key: str = KEY, **declaration: object) -> list[DeliveryContext]:
    """Declare a receiver that answers its first run with ``NotYet(until)`` - the
    offer was extended - and does its work on the next. Returns the contexts it
    was handed."""
    seen: list[DeliveryContext] = []

    def expire(event: Unheard, context: DeliveryContext) -> None:
        seen.append(context)
        if len(seen) == 1:
            raise NotYet(until, reason="the offer was extended")

    receiver(Unheard, key=key, takes_context=True, **declaration)(expire)
    return seen


def test_it_is_exported_from_the_package_root() -> None:
    assert django_domain_events.NotYet is NotYet


def test_it_carries_the_time_and_the_reason() -> None:
    until = _now() + timedelta(hours=24)
    answer = NotYet(until, reason="extended")
    assert (answer.until, answer.reason) == (until, "extended")
    assert "extended" in str(answer)


def test_without_a_reason_it_says_until_when(settings) -> None:
    # The wording, on the setting that keeps the offset; under USE_TZ = False
    # the time is local and says so by having none.
    settings.USE_TZ = True
    until = datetime(2099, 1, 1, tzinfo=timezone.utc)
    assert str(NotYet(until)) == "not due until 2099-01-01T00:00:00+00:00"


def test_a_naive_time_is_refused(settings) -> None:
    """Read as UTC or as local time, it is wrong for somebody by hours - under
    ``USE_TZ = True``. Under ``False`` naive is the project's own convention
    and is accepted, which ``tests/test_naive_datetimes.py`` holds; together
    they hold both halves of the refusal's condition."""
    settings.USE_TZ = True
    with pytest.raises(ValueError, match="NotYet needs an aware datetime"):
        NotYet(datetime(2099, 1, 1))


@pytest.mark.parametrize("until", [3600, "2099-01-01T00:00:00Z", timedelta(hours=1)])
def test_anything_but_a_datetime_is_refused(until: object) -> None:
    """A delay is ``RetryAfter``'s spelling, and the two mean different
    things; a time the application decided is an absolute one."""
    with pytest.raises(TypeError, match="NotYet needs a datetime"):
        NotYet(until)


@pytest.mark.parametrize("ago", [timedelta(0), timedelta(seconds=1), timedelta(days=1)])
def test_a_time_that_is_not_in_the_future_is_refused(ago: timedelta) -> None:
    """A deferral spending no attempt to a time already come would be claimed
    straight back and answered the same way, a hot loop nothing ends."""
    with pytest.raises(ValueError, match="NotYet needs a time in the future"):
        NotYet(_now() - ago)


def test_an_early_run_moves_the_row_to_the_time_and_spends_no_attempt() -> None:
    """The extended offer: the 48-hour row ran, the offer now ends at 72, and
    the row is due again then - with its budget untouched, because nothing
    failed."""
    until = _now() + timedelta(hours=24)
    _extended_to(until, max_attempts=1)
    _fire()

    assert drain_outbox() == {DeliveryStatus.PENDING: 1}

    row = _row()
    assert (row.status, row.attempts) == (DeliveryStatus.PENDING, 0)
    assert (row.available_at, row.due_at) == (until, until)
    assert row.completed_at is None


def test_the_documented_pattern_does_the_work_of_a_row_that_runs_after_the_moved_time() -> None:
    """The comparison the docs teach is ``until > max(ctx.due_at, now)``, and the
    clock term is what this holds: a row due two hours ago whose moved time is
    already an hour gone has nothing left to wait for. Comparing against
    ``ctx.due_at`` alone raises ``NotYet`` with a past time, which is refused
    with a ``ValueError`` and recorded as an ordinary failure - and ``due_at``
    never moves, so every retry would repeat it. Written exactly as documented,
    the receiver does the work, the row succeeds and nothing fails."""
    moved_to = _now() - timedelta(hours=1)
    done: list[int] = []

    def chase(event: Unheard, ctx: DeliveryContext) -> None:
        if moved_to > max(ctx.due_at, django_timezone.now()):
            raise NotYet(moved_to, reason="the quiet period was extended")
        done.append(event.value)

    receiver(Unheard, key=KEY, takes_context=True, due=timedelta(hours=1))(chase)
    _fire(7)
    overdue = _now() - timedelta(hours=2)
    DeliveryRecord.objects.update(available_at=overdue, due_at=overdue)

    assert drain_outbox() == {DeliveryStatus.SUCCEEDED: 1}

    row = _row()
    assert done == [7]
    assert (row.status, row.attempts, row.last_error) == (DeliveryStatus.SUCCEEDED, 1, "")


def test_the_row_is_not_claimed_again_before_the_time() -> None:
    until = _now() + timedelta(hours=24)
    seen = _extended_to(until)
    _fire()
    drain_outbox()

    early = claim_batch(
        worker_id="w", now=until - timedelta(seconds=1), lease=timedelta(minutes=5), limit=10
    )
    assert early == []
    assert drain_outbox() == {}
    assert len(seen) == 1


def test_at_the_time_it_runs_with_the_same_attempt_and_the_new_due_time() -> None:
    """The context carries the time the receiver asked for, so the next run
    compares against it and goes ahead; the attempt number is the one it was
    given before, since none was spent."""
    until = _now() + timedelta(hours=24)
    seen = _extended_to(until)
    _fire()
    drain_outbox()

    assert drain_outbox(now=until) == {DeliveryStatus.SUCCEEDED: 1}

    assert [c.attempt for c in seen] == [1, 1]
    assert seen[1].due_at == until
    assert (_row().status, _row().attempts) == (DeliveryStatus.SUCCEEDED, 1)


def test_a_time_past_the_retry_ceiling_is_not_capped(settings) -> None:
    """``MAX_RECEIVER_RETRY_DELAY_SECONDS`` bounds a destination's advice about
    its own readiness. This is the application's own declaration of when the
    work is due, so it is kept as given, and health shows it as scheduled."""
    settings.DOMAIN_EVENTS = {"MAX_RECEIVER_RETRY_DELAY_SECONDS": 60}
    until = _now() + timedelta(days=30)
    _extended_to(until)
    _fire()

    drain_outbox()

    assert _row().available_at == until
    health = outbox_health()
    assert (health.owed, health.scheduled) == (0, 1)


def test_it_is_not_a_failure_so_on_failure_is_not_told() -> None:
    failures: list[DeliveryFailure] = []
    _extended_to(_now() + timedelta(hours=1), on_failure=failures.append)
    _fire()

    drain_outbox()

    assert failures == []
    assert _row().last_error == ""


def test_give_up_after_does_not_end_it() -> None:
    """``give_up_after`` bounds a throttle the receiver does not control. A due
    time is the application's own, so a row rescheduled past the bound is not
    dead-lettered for it - and a receiver declaring no bound does not have the
    answer counted against its attempts either."""
    until = _now() + timedelta(days=3)
    _extended_to(until, key="probe.bounded", give_up_after=timedelta(minutes=1))
    _extended_to(until, key="probe.unbounded", max_attempts=1)
    _fire()

    drain_outbox()

    for key in ("probe.bounded", "probe.unbounded"):
        row = _row(key)
        assert (row.status, row.attempts, row.due_at) == (DeliveryStatus.PENDING, 0, until)


def test_it_sets_nothing_else_aside() -> None:
    """A deferral pauses its lane because a throttle limits every row behind
    it. This is about one row, so every other row in the batch is delivered."""
    _extended_to(_now() + timedelta(hours=1))
    ran: list[int] = []
    receiver(Unheard, key="probe.neighbour")(lambda event: ran.append(event.value))
    _fire(1)
    _fire(2)

    counts = deliver_pending(worker_id="w")

    assert counts == {DeliveryStatus.PENDING: 1, DeliveryStatus.SUCCEEDED: 3}
    assert sorted(ran) == [1, 2]


def test_a_drain_told_a_later_time_runs_a_rescheduled_row_once() -> None:
    """A receiver deciding by the real clock answers ``NotYet`` on every run of
    a drain told a time beyond its answer, and spends nothing doing so. The pass
    leaves a row it has rescheduled out of its later claims, so it ends.
    Past five calls the receiver raises a BaseException, which nothing in the
    relay catches, so a regression fails here rather than hanging the suite."""
    calls: list[int] = []

    def always_later(event: Unheard) -> None:
        calls.append(1)
        if len(calls) > 5:
            raise _Runaway("the drain kept claiming a rescheduled row")
        raise NotYet(django_timezone.now() + timedelta(hours=1))

    receiver(Unheard, key="probe.clock_reader")(always_later)
    _fire()

    assert drain_outbox(now=_now() + timedelta(days=7)) == {DeliveryStatus.PENDING: 1}


def test_a_direct_call_reschedules_the_row_too() -> None:
    until = _now() + timedelta(hours=1)
    _extended_to(until)
    _fire()

    assert deliver_one(_row().pk) is DeliveryStatus.PENDING
    assert _row().due_at == until
    assert deliver_one(_row().pk) is None


def test_an_inline_receiver_raising_it_fails_the_callers_change() -> None:
    """Read by the relay and by nothing else: an INLINE receiver has no row to
    move, so ``NotYet`` is an ordinary exception there and rolls the change back
    with the event."""

    def early(event: Unheard) -> None:
        raise NotYet(_now() + timedelta(hours=1))

    receiver(Unheard, key="probe.inline", mode=DeliveryMode.INLINE)(early)

    with pytest.raises(NotYet):
        _fire()
    assert EventRecord.objects.count() == 0


@pytest.mark.django_db(transaction=True)
def test_an_on_commit_receiver_raising_it_has_it_logged(caplog) -> None:
    """An ON_COMMIT receiver runs after the change committed, so ``NotYet`` is
    logged as any exception it raises is, and moves nothing."""

    def early(event: Unheard) -> None:
        raise NotYet(_now() + timedelta(hours=1))

    receiver(Unheard, key="probe.on_commit", mode=DeliveryMode.ON_COMMIT)(early)

    with caplog.at_level("ERROR"):
        _fire()

    assert EventRecord.objects.count() == 1
    assert DeliveryRecord.objects.count() == 0
    assert [r.exc_info[0] for r in caplog.records if r.exc_info] == [NotYet]
