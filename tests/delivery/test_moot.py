from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from django.db import transaction

import django_domain_events
from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery.deliver import deliver_one
from django_domain_events.delivery.django_tasks_backend import deliver_delivery
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.moot import Moot
from django_domain_events.introspection.outbox_health import outbox_health
from django_domain_events.introspection.quiet_receivers import quiet_receivers
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.replay_events import replay_events
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.testapp.events import PinnedName, Unheard

pytestmark = pytest.mark.django_db

KEY = "probe.revoke_token"
_ENQUEUED: list[int] = []


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    """Take this file's ad-hoc receivers back out of the process-wide registry."""
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _fire(value: int = 1) -> int:
    with transaction.atomic():
        return fire(Unheard(value=value))


def _row(key: str = KEY) -> DeliveryRecord:
    return DeliveryRecord.objects.get(receiver_key=key)


def _moot(reason: str = "token gone", key: str = KEY, **declaration: object) -> None:
    def revoke(event: Unheard) -> None:
        raise Moot(reason)

    receiver(Unheard, key=key, **declaration)(revoke)


def test_it_is_exported_from_the_package_root() -> None:
    assert django_domain_events.Moot is Moot


def test_it_carries_its_reason() -> None:
    answer = Moot("token gone")
    assert (answer.reason, str(answer)) == ("token gone", "token gone")


def test_a_moot_delivery_is_settled_cancelled_and_not_succeeded() -> None:
    failures: list[DeliveryFailure] = []
    _moot(on_failure=failures.append)
    _fire()

    assert drain_outbox() == {DeliveryStatus.CANCELLED: 1}

    row = _row()
    assert (row.status, row.last_error, row.attempts) == (
        DeliveryStatus.CANCELLED,
        "moot: token gone",
        1,
    )
    assert row.succeeded_at is None
    assert row.completed_at is not None
    assert failures == []


def test_its_attempt_is_the_one_that_raised_it() -> None:
    """A row that failed first records the attempt that found the work moot."""
    runs: list[int] = []

    def flaky(event: Unheard) -> None:
        runs.append(1)
        if len(runs) == 1:
            raise RuntimeError("timeout")
        raise Moot("token gone")

    receiver(Unheard, key=KEY)(flaky)
    _fire()
    drain_outbox()

    assert (_row().status, _row().attempts) == (DeliveryStatus.CANCELLED, 2)


def test_a_long_reason_is_truncated_like_any_error() -> None:
    _moot("x" * 3000)
    _fire()
    drain_outbox()
    assert _row().last_error == ("moot: " + "x" * 3000)[:2000]


def test_deliver_one_returns_cancelled() -> None:
    _moot()
    _fire()
    assert deliver_one(_row().pk) == DeliveryStatus.CANCELLED


def test_its_own_writes_are_rolled_back() -> None:
    def write_then_moot(event: Unheard) -> None:
        fire(PinnedName(value=event.value))
        raise Moot("token gone")

    receiver(Unheard, key=KEY)(write_then_moot)
    _fire()
    drain_outbox()

    assert _row().status == DeliveryStatus.CANCELLED
    assert not EventRecord.objects.filter(name="testapp.pinned").exists()


def test_returning_on_moot_work_reads_as_alive_and_raising_moot_as_quiet() -> None:
    """The bug Moot fixes: returning records a success, so the skipping receiver looks alive."""
    receiver(Unheard, key="probe.returns")(lambda event: None)
    _moot()
    _fire()
    drain_outbox()

    quiet = {q.key for q in quiet_receivers()}
    assert "probe.returns" not in quiet
    assert KEY in quiet


def test_an_eager_attempt_raising_it_settles_the_row_cancelled(
    django_capture_on_commit_callbacks: Any,
) -> None:
    _moot(eager=True)

    with django_capture_on_commit_callbacks(execute=True), transaction.atomic():
        fire(Unheard(value=1))

    row = _row()
    assert (row.status, row.attempts, row.last_error) == (
        DeliveryStatus.CANCELLED,
        1,
        "moot: token gone",
    )
    assert row.succeeded_at is None


def test_a_receiver_whose_deliveries_are_all_moot_reads_as_quiet() -> None:
    """quiet_receivers reads succeeded_at, which a moot delivery never writes."""
    _moot()
    receiver(Unheard, key="probe.works")(lambda event: None)
    _fire(1)
    _fire(2)
    drain_outbox()

    quiet = {q.key: q for q in quiet_receivers()}
    assert quiet[KEY].last_succeeded_at is None
    assert "probe.works" not in quiet


def test_replay_reopens_a_moot_delivery() -> None:
    _moot()
    event_id = _fire()
    drain_outbox()
    assert _row().status == DeliveryStatus.CANCELLED

    counts = replay_events([event_id])

    row = _row()
    assert counts["reopened"] == 1
    assert (row.status, row.last_error, row.completed_at) == (DeliveryStatus.PENDING, "", None)


def test_a_moot_delivery_is_not_counted_as_owed_or_dead() -> None:
    _moot()
    _fire()
    drain_outbox()
    health = outbox_health()
    assert (health.owed, health.dead) == (0, 0)


def test_it_is_logged_at_debug_and_not_above(caplog: pytest.LogCaptureFixture) -> None:
    _moot()
    _fire()

    with caplog.at_level("DEBUG", logger=deliver_module.logger.name):
        drain_outbox()

    said = [r for r in caplog.records if "moot" in r.getMessage()]
    assert [r.levelname for r in said] == ["DEBUG"]
    assert not [
        r for r in caplog.records if r.levelno >= 20 and r.name == deliver_module.logger.name
    ]


class _ImmediateBackend:
    """Runs the task body inside ``enqueue``, as django.tasks' immediate backend does."""

    def enqueue(self, *args: object, **kwargs: object) -> None:
        _ENQUEUED.append(int(args[0]))
        deliver_delivery(*args, **kwargs)


def test_a_task_site_delivery_is_settled_the_same_way(settings) -> None:
    """deliver.py serves the relay and the task worker alike."""
    _ENQUEUED.clear()
    settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": f"{__name__}._ImmediateBackend"}
    _moot(site="task")
    _fire()

    drain_outbox()

    row = _row()
    assert [row.pk] == _ENQUEUED
    assert (row.status, row.last_error, row.succeeded_at) == (
        DeliveryStatus.CANCELLED,
        "moot: token gone",
        None,
    )


def test_an_inline_receiver_raising_it_fails_the_callers_change() -> None:
    """Read by the relay alone: an INLINE Moot is an ordinary exception."""
    _moot(key="probe.inline", mode=DeliveryMode.INLINE)

    with pytest.raises(Moot):
        _fire()
    assert EventRecord.objects.count() == 0


@pytest.mark.django_db(transaction=True)
def test_an_on_commit_receiver_raising_it_has_it_logged(caplog: pytest.LogCaptureFixture) -> None:
    """An ON_COMMIT Moot is logged like any exception, and records nothing."""
    _moot(key="probe.on_commit", mode=DeliveryMode.ON_COMMIT)

    with caplog.at_level("ERROR"):
        _fire()

    assert EventRecord.objects.count() == 1
    assert DeliveryRecord.objects.count() == 0
    assert [r.exc_info[0] for r in caplog.records if r.exc_info] == [Moot]
