"""A single pass over several lanes: one claim per lane, in turn, and a limit shared.

A file of its own beside ``test_deliver.py``, for the reason
``test_run_relay_lanes.py`` gives: every test here needs two lanes with rows of
their own. ``Push`` and ``Sms`` have one receiver each, in the lanes ``push``
and ``sms``, and nothing else listens to them, so the default lane is empty
unless a test fires ``OrderPlaced``. Sorted, the lanes are ``default``,
``push``, ``sms``.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
from django.db import connections, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone as django_timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_pending, dispatch_one
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.retry_after import RetryAfter
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.settings import setting
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import event_registered, receiver_registered, receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)

ORDER: list[str] = []
"""Each delivery's lane, in the order the pass delivered them."""


@dataclass(frozen=True, slots=True)
class Push:
    value: int


@dataclass(frozen=True, slots=True)
class Sms:
    value: int


class _Runaway(BaseException):
    """Raised by a receiver called more often than a working set-aside allows,
    so a broken one fails the test rather than hanging it. A BaseException
    because delivery records every ``Exception`` as a failure."""


def _lane_receiver(lane: str, event_class: type, **options: Any) -> RegisteredReceiver:
    return RegisteredReceiver(
        key=f"tests.{lane}",
        event_class=event_class,
        func=lambda evt: ORDER.append(lane),
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane=lane,
        **options,
    )


@pytest.fixture
def lanes() -> Iterator[None]:
    """``push`` and ``sms``, each with its own lease, and each able to defer
    without counting, which needs ``give_up_after``."""
    ORDER.clear()
    with (
        event_registered(Push, "tests.Push"),
        event_registered(Sms, "tests.Sms"),
        receiver_registered(
            _lane_receiver("push", Push, lease_seconds=60, give_up_after=timedelta(days=1))
        ),
        receiver_registered(
            _lane_receiver("sms", Sms, lease_seconds=30, give_up_after=timedelta(days=1))
        ),
    ):
        yield
    ORDER.clear()


def _fire(event_class: type, count: int = 1) -> None:
    with transaction.atomic():
        for value in range(count):
            fire(event_class(value=value))


@contextmanager
def _claims(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[dict[str, Any]]]:
    """Every claim the pass makes, as the keyword arguments it made it with."""
    made: list[dict[str, Any]] = []
    real = deliver_module.claim_batch

    def spy(**kwargs: Any) -> list[int]:
        made.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(deliver_module, "claim_batch", spy)
    yield made


def _deferring(seen: list[int], *, times: int = 1) -> Callable[[Sms], None]:
    """An SMS receiver whose destination asks for a pause on its first ``times``
    calls and accepts the rest."""

    def sms(evt: Sms) -> None:
        seen.append(evt.value)
        if len(seen) > 10:
            raise _Runaway("the set-aside lane kept being attempted")
        if len(seen) <= times:
            raise RetryAfter(3600, reason="credit", counts=False)
        ORDER.append("sms")

    return sms


def _delivered() -> dict[str, int]:
    return {lane: ORDER.count(lane) for lane in sorted(set(ORDER))}


# --- Rotation: one claim per lane, in turn -------------------------------------


def test_a_burst_in_one_lane_does_not_hold_back_another_under_every_lane(lanes: None) -> None:
    """Thirty push rows owed before one SMS row, claimed ten at a time. One
    claim across every lane, in ``available_at`` order, would deliver every
    push row first; one claim per lane delivers SMS within one push batch."""
    _fire(Push, 30)
    _fire(Sms)

    assert deliver_pending(batch_size=10) == {DeliveryStatus.SUCCEEDED: 31}

    assert ORDER.index("sms") <= 10, "the SMS row waited behind the push burst"


def test_a_lanes_wait_is_one_batch_of_each_other_lane_not_their_backlog(lanes: None) -> None:
    """``push`` sized at three in a set: every SMS row waits for at most three
    push deliveries, however many push rows are owed."""
    _fire(Push, 30)
    _fire(Sms, 3)

    deliver_pending(lane=["push", "sms"], lane_batch_sizes={"push": 3, "sms": 1})

    assert ORDER[:12] == ["push", "push", "push", "sms"] * 3
    assert _delivered() == {"push": 30, "sms": 3}


def test_every_lane_is_claimed_under_its_own_lease(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``push`` declares 60 seconds and ``sms`` 30. One claim across every lane
    would stamp ``LEASE_SECONDS`` on both, since the two differ."""
    _fire(Push)
    _fire(Sms)

    with _claims(monkeypatch) as made:
        deliver_pending()

    leases = {claim["lane"]: claim["lease"] for claim in made}
    assert leases.get("push") == timedelta(seconds=66)
    assert leases.get("sms") == timedelta(seconds=33)


def test_a_set_is_served_once_per_lane_in_sorted_order_until_a_rotation_claims_nothing(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Duplicates name the same work. The call ends after the first rotation in
    which no lane claimed anything, and a lane that claimed nothing in one
    rotation is still asked in the next while another had work."""
    _fire(Push, 3)

    with _claims(monkeypatch) as made:
        deliver_pending(lane=["sms", "push", "sms"], batch_size=2)

    assert [(claim["lane"], claim["limit"]) for claim in made] == [
        ("push", 2),
        ("sms", 2),
        ("push", 2),
        ("sms", 2),
        ("push", 2),
        ("sms", 2),
    ]
    assert _delivered() == {"push": 3}


def test_a_deferral_sets_its_lane_aside_and_the_next_lane_is_still_claimed(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``push`` defers at its first row and sorts before ``sms``: its other rows
    go straight back, it is not claimed again, and ``sms`` is delivered in the
    same call."""
    seen: list[int] = []

    def push(evt: Push) -> None:
        seen.append(evt.value)
        if len(seen) > 1:
            raise _Runaway("the set-aside lane kept being attempted")
        raise RetryAfter(3600, reason="throttled", counts=False)

    _fire(Push, 3)
    _fire(Sms, 2)
    with receiver_replaced("tests.push", push), _claims(monkeypatch) as made:
        counts = deliver_pending(lane=["push", "sms"])

    assert counts == {DeliveryStatus.FAILED: 1, DeliveryStatus.SUCCEEDED: 2}
    assert seen == [0]
    assert _delivered() == {"sms": 2}
    assert [claim["lane"] for claim in made] == ["push", "sms", "sms"]
    assert (
        DeliveryRecord.objects.filter(
            receiver_key="tests.push",
            status=DeliveryStatus.CLAIMED,
            lease_expires_at__lt=django_timezone.now(),
        ).count()
        == 2
    ), "the rest of the deferring lane's batch was not handed back"


def test_a_deferral_asks_nothing_about_the_rows_it_hands_back(lanes: None) -> None:
    """Under every lane, each batch is one lane's, so a deferral hands back the
    rest of it with no query to sort out which rows are the lane's. That query
    would select each row's ``receiver_key`` by its id, over the whole batch."""
    _fire(Sms, 3)

    with (
        receiver_replaced("tests.sms", _deferring([])),
        CaptureQueriesContext(connections[write_alias()]) as queries,
    ):
        assert deliver_pending() == {DeliveryStatus.FAILED: 1}

    split = re.compile(r'^SELECT .*"receiver_key" FROM \S*deliveryrecord\S* WHERE .*"id" IN \(')
    assert [q["sql"] for q in queries.captured_queries if split.match(q["sql"])] == []
    assert (
        DeliveryRecord.objects.filter(
            receiver_key="tests.sms", lease_expires_at__lt=django_timezone.now()
        ).count()
        == 2
    ), "the rest of the batch was not handed back"


# --- A limit is the call's total, shared between the lanes with work ----------


@pytest.mark.parametrize("lane", [None, ["push", "sms"]], ids=["every-lane", "a-set"])
def test_a_limit_is_shared_between_the_lanes_with_work(lanes: None, lane: list[str] | None) -> None:
    """Twenty owed in each, push's fired first: ten split five and five, rather
    than ten push rows because they are older. Under every lane, ``default``
    is empty, takes nothing of its share and leaves it to the others."""
    _fire(Push, 20)
    _fire(Sms, 20)

    assert deliver_pending(limit=10, lane=lane) == {DeliveryStatus.SUCCEEDED: 10}

    assert _delivered() == {"push": 5, "sms": 5}


def test_a_drained_lanes_share_goes_to_the_lanes_with_work(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two owed in ``sms``: it takes two of its five and is drained, and push
    takes the three left in a second rotation."""
    _fire(Push, 20)
    _fire(Sms, 2)

    with _claims(monkeypatch) as made:
        deliver_pending(limit=10, lane=["push", "sms"])

    assert _delivered() == {"push": 8, "sms": 2}
    assert [(claim["lane"], claim["limit"]) for claim in made] == [
        ("push", 5),
        ("sms", 5),
        ("push", 3),
    ]


def test_a_limit_below_the_number_of_lanes_serves_the_first_lanes(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One row over two lanes goes to the first in sorted order, and the second
    is not asked for none."""
    _fire(Push, 2)
    _fire(Sms, 2)

    with _claims(monkeypatch) as made:
        assert deliver_pending(limit=1, lane=["push", "sms"]) == {DeliveryStatus.SUCCEEDED: 1}

    assert _delivered() == {"push": 1}
    assert [claim["lane"] for claim in made] == ["push"]


def test_rows_handed_back_on_a_deferral_do_not_count_against_the_limit(lanes: None) -> None:
    """``sms`` takes five of the ten, defers at the first and hands four back.
    Four of the limit are left, not none, and push takes them."""
    seen: list[int] = []
    _fire(Push, 20)
    _fire(Sms, 5)

    with receiver_replaced("tests.sms", _deferring(seen)):
        deliver_pending(limit=10, lane=["push", "sms"])

    assert seen == [0]
    assert _delivered() == {"push": 9}


def test_with_one_lane_a_limit_is_one_claim_of_that_many(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The lane's share is the whole limit, and a claim either spends it or
    finds the lane drained, so there is never a second."""
    _fire(Push, 5)

    with _claims(monkeypatch) as made:
        assert deliver_pending(limit=3, lane="push") == {DeliveryStatus.SUCCEEDED: 3}
        assert deliver_pending(limit=3, lane="push") == {DeliveryStatus.SUCCEEDED: 2}

    assert [claim["limit"] for claim in made] == [3, 3]


def test_a_limit_with_lane_sizes_is_refused(lanes: None) -> None:
    """Each lane's claim is its share of the limit, so the sizes would be
    ignored. Each alone is accepted, which holds both conjuncts."""
    _fire(Push, 2)

    with pytest.raises(ValueError, match="limit=10 sizes each claim as its lane.s share of it"):
        deliver_pending(limit=10, lane=["push", "sms"], lane_batch_sizes={"push": 1})
    assert deliver_pending(limit=1, lane=["push", "sms"]) == {DeliveryStatus.SUCCEEDED: 1}
    assert deliver_pending(lane=["push", "sms"], lane_batch_sizes={"push": 1}) == {
        DeliveryStatus.SUCCEEDED: 1
    }


# --- Refusals ------------------------------------------------------------------


def test_an_undeclared_member_is_refused_by_name(lanes: None) -> None:
    with pytest.raises(ValueError, match="No receiver is declared in lane 'pusj'"):
        deliver_pending(lane=["sms", "pusj"])


def test_an_empty_set_of_lanes_is_refused(lanes: None) -> None:
    with pytest.raises(ValueError, match="lane is an empty collection"):
        deliver_pending(lane=[])


def test_a_lane_that_is_not_a_string_is_refused(lanes: None) -> None:
    with pytest.raises(ValueError, match="A lane is named by a str, got 3"):
        deliver_pending(lane=["sms", 3])


def test_a_size_for_a_lane_not_served_is_refused(lanes: None) -> None:
    with pytest.raises(ValueError, match="lane_batch_sizes names lane 'push'"):
        deliver_pending(lane="sms", lane_batch_sizes={"push": 3})


@pytest.mark.parametrize("size", [0, -1])
def test_a_lane_size_below_one_is_refused(lanes: None, size: int) -> None:
    with pytest.raises(ValueError, match=r"lane_batch_sizes\['push'\] must be positive"):
        deliver_pending(lane=["push", "sms"], lane_batch_sizes={"push": size})


# --- What does not change ------------------------------------------------------


@contextmanager
def _statements() -> Iterator[list[str]]:
    """The SQL sent on the write connection, as templates with placeholders,
    because primary keys and timestamps differ between two identical passes."""
    sent: list[str] = []

    def capture(
        execute: Callable[..., Any], sql: str, params: Any, many: bool, context: Any
    ) -> Any:
        sent.append(sql)
        return execute(sql, params, many, context)

    with connections[write_alias()].execute_wrapper(capture):
        yield sent


def _the_pass_without_lanes(*, lane: str | None, ignore_backoff: bool = False) -> None:
    """``deliver_pending`` as it ran before a pass served several lanes: claims
    of ``BATCH_SIZE`` for ``lane``, with nothing set aside, each followed by a
    delivery per row, until one comes back empty. Run against that
    ``deliver_pending`` and found equal to it, which is why the tests below can
    compare against it on any backend and Django."""
    while True:
        ids = claim_batch(
            worker_id="deliver_pending",
            now=django_timezone.now(),
            lease=timedelta(
                seconds=registry.claim_lease_seconds(lane, default=setting("LEASE_SECONDS"))
            ),
            limit=setting("BATCH_SIZE"),
            ignore_backoff=ignore_backoff,
            lane=lane,
            exclude_lanes=set(),
            exclude_ids=set(),
        )
        for delivery_id in ids:
            dispatch_one(delivery_id, worker_id="deliver_pending", on_deferral=lambda *_: None)
        if not ids:
            return


def test_a_single_lane_pass_sends_what_it_always_sent(lanes: None) -> None:
    """A single lane given as a ``str``, with no deferral: the same statements,
    in the same order and number, as the claims and deliveries a pass made
    before it served several lanes."""
    _fire(Sms, 3)
    with _statements() as passed:
        assert deliver_pending(lane="sms") == {DeliveryStatus.SUCCEEDED: 3}

    _fire(Sms, 3)
    with _statements() as before:
        _the_pass_without_lanes(lane="sms")

    assert passed == before


@pytest.mark.parametrize(
    ("run", "ignore_backoff"),
    [(deliver_pending, False), (drain_outbox, True)],
    ids=["deliver_pending", "drain_outbox"],
)
def test_with_no_named_lanes_every_lane_sends_what_it_always_sent(
    order: OrderPlaced,
    record: list[str],
    monkeypatch: pytest.MonkeyPatch,
    run: Callable[[], dict[DeliveryStatus, int]],
    ignore_backoff: bool,
) -> None:
    """``lane=None`` in a project that names no lane is the default lane alone,
    whose condition compiles to no filter at all and whose lease is every
    receiver's: the same claims across every lane that None always made. The
    statements are compared as templates, so the lease, a parameter, is
    compared on its own."""
    assert registry.lanes() == ["default"]
    for _ in range(2):
        with transaction.atomic():
            fire(order)
    with _claims(monkeypatch) as made, _statements() as passed:
        assert run() == {DeliveryStatus.SUCCEEDED: 4}

    for _ in range(2):
        with transaction.atomic():
            fire(order)
    with _statements() as before:
        _the_pass_without_lanes(lane=None, ignore_backoff=ignore_backoff)

    assert passed == before
    lease = timedelta(seconds=registry.claim_lease_seconds(None, default=setting("LEASE_SECONDS")))
    assert [claim["lease"] for claim in made] == [lease, lease]
