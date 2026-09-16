from __future__ import annotations

import hashlib
import importlib
import secrets
from collections.abc import Iterator
from unittest import mock

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.replay_events import replay_events
from django_domain_events.scope.attributed import attributed
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import receiver_deleted
from tests.testapp.events import OrderPlaced, Unheard

pytestmark = pytest.mark.django_db(transaction=True)


def _status(key: str) -> str:
    return DeliveryRecord.objects.values_list("status", flat=True).get(receiver_key=key)


def test_a_delivered_event_can_be_made_owed_again(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()
    record.clear()

    assert replay_events([event_id]) == {"reopened": 2, "added": 0}
    assert _status("testapp.durable_receiver") == DeliveryStatus.PENDING

    drain_outbox()
    assert "durable:7" in record


def test_reopening_clears_the_previous_attempt(order: OrderPlaced, record: list[str]) -> None:
    """A row carrying its old attempt count and error would dead-letter on the
    first failure and tell an operator nothing new."""
    with transaction.atomic():
        event_id = fire(order)
    DeliveryRecord.objects.update(
        status=DeliveryStatus.DEAD, attempts=5, last_error="boom", claimed_by="w1"
    )

    replay_events([event_id])
    row = DeliveryRecord.objects.first()
    assert (row.attempts, row.last_error, row.claimed_by) == (0, "", "")


def test_a_receiver_registered_later_can_be_given_the_backlog(
    order: OrderPlaced, record: list[str]
) -> None:
    """The other half of freezing the receiver set at fire time: a deploy does
    not hand a new receiver a week of events, and this is how you choose to."""
    with receiver_deleted("testapp.with_context"):
        with transaction.atomic():
            event_id = fire(order)
        assert DeliveryRecord.objects.count() == 1

    assert replay_events([event_id]) == {"reopened": 0, "added": 1}
    assert _status("testapp.with_context") == DeliveryStatus.PENDING


def test_a_delivery_in_flight_is_left_alone(order: OrderPlaced, record: list[str]) -> None:
    """Reopening a claimed row would hand the same work to two receivers, which
    is the one thing the lease exists to prevent."""
    with transaction.atomic():
        event_id = fire(order)
    DeliveryRecord.objects.update(status=DeliveryStatus.CLAIMED)

    assert replay_events([event_id]) == {"reopened": 0, "added": 0}


def test_it_can_be_narrowed_to_one_receiver(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()

    replay_events([event_id], receiver_keys=["testapp.durable_receiver"])
    assert _status("testapp.durable_receiver") == DeliveryStatus.PENDING
    assert _status("testapp.with_context") == DeliveryStatus.SUCCEEDED


def test_an_event_whose_class_is_gone_is_skipped(order: OrderPlaced, record: list[str]) -> None:
    """Nothing can be replayed for a name the registry no longer has, and
    failing the whole batch over one of them helps nobody."""
    with transaction.atomic():
        event_id = fire(order)
    from django_domain_events.models.event_record import EventRecord

    EventRecord.objects.filter(pk=event_id).update(name="testapp.Retired")

    assert replay_events([event_id]) == {"reopened": 0, "added": 0}


def test_it_will_not_wipe_a_live_claim(order: OrderPlaced, record: list[str]) -> None:
    """The status predicate on the update, not only on the read.

    Interleaved for real: an earlier version claimed the row before calling, so
    the read already saw CLAIMED and the update never targeted it - it passed
    with the predicate removed. The membership test that decides what to reopen
    runs between the read and the write, so the steal is hooked there.
    """
    module = importlib.import_module("django_domain_events.operations.replay_events")

    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()
    stolen_id = DeliveryRecord.objects.order_by("pk").values_list("pk", flat=True).first()

    real_terminal = module._TERMINAL

    class StealWhenAsked(tuple):
        def __contains__(self, item: object) -> bool:
            DeliveryRecord.objects.filter(pk=stolen_id).update(
                status=DeliveryStatus.CLAIMED, claimed_by="relay-b"
            )
            return tuple.__contains__(self, item)

    with mock.patch.object(module, "_TERMINAL", StealWhenAsked(real_terminal)):
        assert replay_events([event_id])["reopened"] == 1

    stolen = DeliveryRecord.objects.get(pk=stolen_id)
    assert (stolen.status, stolen.claimed_by) == (DeliveryStatus.CLAIMED, "relay-b")


def test_one_events_collision_does_not_discard_the_others(
    order: OrderPlaced, record: list[str]
) -> None:
    """One transaction for the whole call meant a conflict on any single event
    threw away the reopens for every other event the operator named."""
    with transaction.atomic():
        first = fire(order)
        second = fire(order)
    drain_outbox()

    counts = replay_events([first, second])
    assert counts["reopened"] == 4


def test_it_wakes_a_waiting_relay(order: OrderPlaced, record: list[str]) -> None:
    """Replay makes rows owed exactly as fire() does, so it has to reach the same
    low-latency path; otherwise replayed work sits until the next poll."""
    # Patched on the module object, not by dotted path: `__init__` re-exports
    # `replay_events`, so the package attribute of that name is the function and
    # `mock.patch("django_domain_events.operations.replay_events.notify_relay")` walks into
    # the function rather than the module.
    module = importlib.import_module("django_domain_events.operations.replay_events")
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()

    with mock.patch.object(module, "notify_relay") as notify:
        replay_events([event_id])
    assert notify.called


def test_it_does_not_wake_anything_when_nothing_changed(
    order: OrderPlaced, record: list[str]
) -> None:
    module = importlib.import_module("django_domain_events.operations.replay_events")
    with transaction.atomic():
        event_id = fire(order)

    with mock.patch.object(module, "notify_relay") as notify:
        replay_events([event_id])
    assert not notify.called


@pytest.fixture
def owed_targets() -> Iterator[list[str]]:
    """A fan-out receiver whose targets a test can change between fire and replay.

    The callable reads this list each time it is called, so rewriting it is how
    a test says "the destinations are different now".
    """
    current = ["a", "b"]
    receiver(Unheard, key="probe.fan", targets=lambda event, context: list(current))(
        lambda event: None
    )
    yield current
    registry._receivers.pop("probe.fan", None)


def _fan_rows() -> list[tuple[str, str, int]]:
    return list(
        DeliveryRecord.objects.filter(receiver_key="probe.fan")
        .order_by("target")
        .values_list("target", "status", "attempts")
    )


def test_a_fan_out_replay_goes_to_the_targets_that_exist_now(owed_targets: list[str]) -> None:
    """Re-derived rather than re-run: ``b`` is still owed and is reopened, ``c``
    is new and is added, and ``a`` - no longer returned - is left exactly as it
    was, neither reopened nor counted."""
    with transaction.atomic():
        event_id = fire(Unheard(value=1))
    drain_outbox()
    owed_targets[:] = ["b", "c"]

    assert replay_events([event_id]) == {"reopened": 1, "added": 1}
    assert _fan_rows() == [
        ("a", DeliveryStatus.SUCCEEDED, 1),
        ("b", DeliveryStatus.PENDING, 0),
        ("c", DeliveryStatus.PENDING, 0),
    ]


def test_a_blank_row_from_before_the_receiver_fanned_out_is_left_alone() -> None:
    """The row every receiver had before ``targets=`` existed.

    A receiver that gains a callable has said the event is owed to what the
    callable returns, and the blank row is not among it.
    """
    receiver(Unheard, key="probe.fan")(lambda event: None)
    try:
        with transaction.atomic():
            event_id = fire(Unheard(value=1))
        drain_outbox()
        object.__setattr__(
            registry.receiver_for_key("probe.fan"), "targets", lambda event, context: ["a"]
        )

        assert replay_events([event_id]) == {"reopened": 0, "added": 1}
        assert _fan_rows() == [("", DeliveryStatus.SUCCEEDED, 1), ("a", DeliveryStatus.PENDING, 0)]
    finally:
        registry._receivers.pop("probe.fan", None)


def test_the_callable_is_handed_the_rebuilt_event_and_the_recorded_context() -> None:
    seen: list[tuple[object, DeliveryContext]] = []

    def targets(event: Unheard, context: DeliveryContext) -> list[str]:
        seen.append((event, context))
        return ["a"]

    receiver(Unheard, key="probe.fan", targets=targets)(lambda event: None)
    try:
        with transaction.atomic(), attributed(actor_key="auth.User:9", tenant="acme"):
            event_id = fire(Unheard(value=4))
        seen.clear()

        replay_events([event_id])

        [(event, context)] = seen
        assert event == Unheard(value=4)
        assert (context.event_id, context.event_name, context.attempt, context.target) == (
            event_id,
            "testapp.Unheard",
            1,
            "",
        )
        assert (context.actor_key, context.scope) == ("auth.User:9", {"tenant": "acme"})
    finally:
        registry._receivers.pop("probe.fan", None)


def test_a_fan_out_replay_of_a_payload_that_no_longer_decodes_raises(
    owed_targets: list[str],
) -> None:
    """Loud, to the operator who asked, rather than an empty replay."""
    with transaction.atomic():
        event_id = fire(Unheard(value=1))
    EventRecord.objects.filter(pk=event_id).update(payload={})

    with pytest.raises(TypeError, match="missing 1 required positional argument"):
        replay_events([event_id])


def test_a_plain_receiver_replay_never_rebuilds_the_event(
    order: OrderPlaced, record: list[str]
) -> None:
    """The rebuild is for the callable, so only a fan-out pays for it. A plain
    receiver's reopened row dead-letters in the relay, as it always has."""
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()
    EventRecord.objects.filter(pk=event_id).update(payload={})

    assert replay_events([event_id]) == {"reopened": 2, "added": 0}


def test_a_row_replay_adds_carries_the_digest_of_its_target(owed_targets: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(Unheard(value=1))
    owed_targets[:] = ["a", "b", "c" * 3000]

    assert replay_events([event_id]) == {"reopened": 0, "added": 1}
    added = DeliveryRecord.objects.get(receiver_key="probe.fan", target="c" * 3000)
    assert added.target_digest == hashlib.sha256(("c" * 3000).encode()).hexdigest()


def test_a_long_target_already_delivered_is_reopened_not_duplicated(
    owed_targets: list[str],
) -> None:
    """Matched by digest, so ten thousand characters are found as one row."""
    long_target = secrets.token_urlsafe(7500)[:10_000]
    owed_targets[:] = [long_target]
    with transaction.atomic():
        event_id = fire(Unheard(value=1))
    drain_outbox()

    assert replay_events([event_id]) == {"reopened": 1, "added": 0}
    assert _fan_rows() == [(long_target, DeliveryStatus.PENDING, 0)]


def test_replay_finds_existing_rows_by_the_indexed_digest_not_the_text(
    owed_targets: list[str],
) -> None:
    """The digest is what the unique index covers; the text is in no index, so
    a lookup by it scans every delivery of the event."""
    with transaction.atomic():
        event_id = fire(Unheard(value=1))
    drain_outbox()

    with CaptureQueriesContext(connection) as queries:
        replay_events([event_id], receiver_keys=["probe.fan"])

    lookups = [
        q["sql"]
        for q in queries.captured_queries
        if q["sql"].startswith("SELECT") and "django_domain_events_deliveryrecord" in q["sql"]
    ]
    assert lookups, "replay read the existing rows"
    for sql in lookups:
        where = sql.split(" WHERE ", 1)[1]
        assert '"target_digest" IN' in where
        assert '"target" IN' not in where
