from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from django.db import transaction
from django.utils import timezone

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_one
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.not_yet import NotYet
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.replay_events import replay_events
from django_domain_events.scope.attributed import attributed
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db

HAPPENED = timezone.now() - timedelta(days=3)
CORRELATION = uuid4()


@pytest.fixture(autouse=True)
def _no_leaked_receivers() -> Iterator[None]:
    yield
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _fire() -> int:
    with attributed(correlation_id=CORRELATION), transaction.atomic():
        event_id = fire(Unheard(value=1), occurred_at=HAPPENED)
    assert event_id is not None
    return event_id


def _watch(key: str, **declaration: Any) -> list[DeliveryContext]:
    seen: list[DeliveryContext] = []

    def read(event: Unheard, context: DeliveryContext) -> None:
        seen.append(context)

    receiver(Unheard, key=key, takes_context=True, **declaration)(read)
    return seen


def _assert_carries(seen: list[DeliveryContext]) -> None:
    assert [(c.occurred_at, c.correlation_id) for c in seen] == [(HAPPENED, CORRELATION)]


def test_the_relay_hands_the_event_times_and_correlation() -> None:
    seen = _watch("probe.relay")
    _fire()
    run_relay(worker_id="w1", passes=1, allow_unsafe_concurrency=True)
    _assert_carries(seen)


def test_a_task_delivery_hands_them() -> None:
    seen = _watch("probe.task", site="task")
    _fire()
    row = DeliveryRecord.objects.get(receiver_key="probe.task")
    claim_batch(worker_id="w1", now=timezone.now(), lease=timedelta(minutes=5), limit=10)
    row.refresh_from_db()
    assert row.claimed_at is not None
    deliver_one(row.pk, worker_id="w1")
    _assert_carries(seen)


def test_an_on_commit_receiver_gets_them(
    django_capture_on_commit_callbacks: Callable[..., Any],
) -> None:
    seen = _watch("probe.commit", mode=DeliveryMode.ON_COMMIT)
    with django_capture_on_commit_callbacks(execute=True):
        _fire()
    _assert_carries(seen)


def test_an_inline_receiver_gets_them() -> None:
    seen = _watch("probe.inline", mode=DeliveryMode.INLINE)
    _fire()
    _assert_carries(seen)


def test_a_targets_callable_gets_them() -> None:
    seen: list[DeliveryContext] = []

    def targets(event: Unheard, context: DeliveryContext) -> list[str]:
        seen.append(context)
        return ["a"]

    receiver(Unheard, key="probe.fan", targets=targets)(lambda event: None)
    _fire()
    _assert_carries(seen)


def test_a_replay_asks_targets_with_them() -> None:
    seen: list[DeliveryContext] = []

    def targets(event: Unheard, context: DeliveryContext) -> list[str]:
        seen.append(context)
        return ["a"]

    receiver(Unheard, key="probe.fan", targets=targets)(lambda event: None)
    event_id = _fire()
    seen.clear()
    replay_events([event_id])
    _assert_carries(seen)


def test_a_null_correlation_is_none() -> None:
    seen = _watch("probe.inline", mode=DeliveryMode.INLINE)
    with transaction.atomic():
        fire(Unheard(value=1))
    assert EventRecord.objects.get().correlation_id is None
    assert seen[0].correlation_id is None
    assert seen[0].occurred_at is not None


def test_a_context_built_by_hand_with_the_old_arguments_still_works() -> None:
    context = DeliveryContext(
        event_id=1, event_name="x", attempt=1, actor_key="", actor_label="", scope={}
    )
    assert (context.occurred_at, context.correlation_id) == (None, None)


@pytest.mark.parametrize(
    "given",
    [
        pytest.param(HAPPENED.astimezone(ZoneInfo("Asia/Tokyo")), id="aware-not-utc"),
        pytest.param(HAPPENED.replace(tzinfo=None), id="naive"),
    ],
)
@pytest.mark.filterwarnings("ignore:DateTimeField .* received a naive datetime")
def test_inline_and_relay_hand_the_occurred_at_the_row_stores(
    given: datetime, settings: Any
) -> None:
    inline = _watch("probe.inline", mode=DeliveryMode.INLINE)
    relay = _watch("probe.relay")
    with transaction.atomic():
        fire(Unheard(value=1), occurred_at=given)
    run_relay(worker_id="w1", passes=1, allow_unsafe_concurrency=True)
    stored = EventRecord.objects.get().occurred_at
    assert [(c.occurred_at, c.occurred_at.isoformat()) for c in inline + relay] == [
        (stored, stored.isoformat())
    ] * 2
    if settings.USE_TZ:
        assert inline[0].occurred_at.tzinfo is not None


def test_a_not_yet_reschedule_keeps_occurred_at_and_moves_due_at() -> None:
    until = timezone.now() + timedelta(hours=1)
    seen: list[DeliveryContext] = []

    def defer_once(event: Unheard, context: DeliveryContext) -> None:
        seen.append(context)
        if len(seen) == 1:
            raise NotYet(until)

    receiver(Unheard, key="probe.wait", takes_context=True)(defer_once)
    _fire()
    drain_outbox()
    drain_outbox(now=until)
    assert [(c.occurred_at, c.due_at) for c in seen][1] == (HAPPENED, until)
    assert seen[0].occurred_at == HAPPENED
