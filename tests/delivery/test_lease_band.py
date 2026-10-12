"""One write per delivery for the lease.

A claim stamps the lease its lane needs, with a fixed headroom, and a delivery
whose claimed lease already is what its receiver needs starts without the
extension: a write to ``lease_expires_at``, which ``dde_claimed_by_lease`` is
keyed on, so never a HOT update on Postgres.

The extension is recognised in the captured SQL as an UPDATE of the delivery
table setting ``lease_expires_at`` and nothing else. The claim sets four
columns, the take two others and an outcome never this one, so none of them
matches; ``hand_back`` would, and nothing here hands back.

Probes are declared on ``Unheard``, which the test app never listens to, in a
lane of their own, so no test app receiver shares a claim with them.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any
from unittest import mock

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone as django_timezone

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.delivery import claim_batch as claim_module
from django_domain_events.delivery import deliver as deliver_module
from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_one, deliver_pending, dispatch_one
from django_domain_events.delivery.django_tasks_backend import deliver_delivery
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_status import DeliveryStatus
from tests.conftest import receiver_deleted
from tests.testapp.events import Unheard

pytestmark = pytest.mark.django_db(transaction=True)

_LEASE_ONLY = re.compile(r'^UPDATE "[^"]*deliveryrecord" SET "lease_expires_at" = [^,]+? WHERE')

RAN: list[str] = []
MESSAGES: list[Any] = []


class RecordingBackend:
    """Keeps each message as JSON, the only form every queue carries, so a test
    can replay it into the task body as a worker would, when it chooses to."""

    def enqueue(self, *args: object, **kwargs: object) -> None:
        MESSAGES.append(json.loads(json.dumps([args, kwargs])))


class InlineBackend:
    """Runs the task body inside the hand-off, as an immediate backend does."""

    def enqueue(self, delivery_id: int, claimed_by: str, claimed_at: str) -> None:
        deliver_delivery(delivery_id, claimed_by, claimed_at)


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    RAN.clear()
    MESSAGES.clear()
    yield
    RAN.clear()
    MESSAGES.clear()
    for key in [key for key in registry._receivers if key.startswith("probe.")]:
        registry._receivers.pop(key)


def _probe(key: str, **options: Any) -> None:
    options.setdefault("lane", "mail")
    receiver(Unheard, key=f"probe.{key}", **options)(lambda event: RAN.append(key))


def _fire(count: int = 1) -> None:
    with transaction.atomic():
        for value in range(count):
            fire(Unheard(value=value))


def _lease_writes(queries: CaptureQueriesContext) -> int:
    return sum(1 for q in queries.captured_queries if _LEASE_ONLY.match(q["sql"]))


def _rows() -> list[DeliveryRecord]:
    return list(DeliveryRecord.objects.order_by("pk"))


@contextmanager
def _claimed_leases(module: Any) -> Iterator[list[timedelta]]:
    """Every ``lease=`` a claim through ``module`` is made with, in order."""
    leases: list[timedelta] = []
    real = claim_module.claim_batch

    def recording(**kwargs: Any) -> list[int]:
        leases.append(kwargs["lease"])
        return real(**kwargs)

    with mock.patch.object(module, "claim_batch", recording):
        yield leases


def _relay(**options: Any) -> None:
    options.setdefault("lane", "mail")
    run_relay(
        worker_id="relay",
        passes=1,
        allow_unsafe_concurrency=True,
        wait=lambda timeout: False,
        sleep=lambda seconds: None,
        **options,
    )


# The lease a claim stamps.


def test_a_uniform_lane_is_claimed_under_its_own_lease() -> None:
    """Every receiver in the lane declares 60 s, so the claim stamps 60 s and
    its headroom rather than ``LEASE_SECONDS``."""
    _probe("a", lease_seconds=60)
    _probe("b", lease_seconds=60)
    _fire()

    with _claimed_leases(deliver_module) as leases:
        deliver_pending(lane="mail")

    assert leases[0] == timedelta(seconds=66)


def test_a_mixed_lane_is_claimed_under_the_setting() -> None:
    _probe("a", lease_seconds=60)
    _probe("b", lease_seconds=90)
    _fire()

    with _claimed_leases(deliver_module) as leases:
        deliver_pending(lane="mail")

    assert leases[0] == timedelta(seconds=330)


def test_a_lane_of_receivers_declaring_no_lease_is_claimed_under_the_setting() -> None:
    """Uniform at the setting is uniform too: a receiver that declares none
    needs ``LEASE_SECONDS``, so the claim stamps that and its headroom."""
    _probe("a")
    _probe("b")
    _fire()

    with _claimed_leases(deliver_module) as leases:
        deliver_pending(lane="mail")

    assert leases[0] == timedelta(seconds=330)


def test_every_lane_at_once_is_claimed_under_the_setting() -> None:
    """``lane=None`` takes every receiver's rows, and the test app's declare
    1800 s as well as none at all."""
    _probe("a", lease_seconds=60)
    _fire()

    with _claimed_leases(deliver_module) as leases:
        deliver_pending()

    assert leases[0] == timedelta(seconds=330)


def test_a_relay_claims_a_uniform_lane_under_its_own_lease() -> None:
    _probe("a", lease_seconds=60)
    _fire()

    with _claimed_leases(run_relay_module) as leases:
        _relay()

    assert leases == [timedelta(seconds=66)]


def test_the_default_lane_relay_is_claimed_under_the_setting() -> None:
    """The test app's default lane mixes 1800 s with the setting."""
    with _claimed_leases(run_relay_module) as leases:
        _relay(lane="default")

    assert leases == [timedelta(seconds=330)]


def test_the_eager_attempt_is_claimed_under_its_receivers_lease() -> None:
    """The attempt after commit claims only eager receivers' rows, so their
    shared lease is its base. It then starts without an extension."""
    _probe("eager", eager=True, lease_seconds=60)

    with (
        # The test app's own eager receiver needs the setting, and would make
        # the eager receivers a mix.
        receiver_deleted("testapp.eager"),
        _claimed_leases(claim_module) as leases,
        CaptureQueriesContext(connection) as queries,
    ):
        _fire()

    assert leases == [timedelta(seconds=66)]
    assert RAN == ["eager"]
    assert _lease_writes(queries) == 0


# The skip.


def test_a_row_claimed_for_its_receiver_starts_without_extending_its_lease() -> None:
    _probe("a", lease_seconds=60)
    _fire(3)

    with CaptureQueriesContext(connection) as queries:
        counts = deliver_pending(lane="mail")

    assert counts == {DeliveryStatus.SUCCEEDED: 3}
    assert _lease_writes(queries) == 0
    for row in _rows():
        # Still the claim's own stamp: nothing rewrote it.
        assert row.lease_expires_at - row.claimed_at == timedelta(seconds=66)


def test_a_relay_delivers_a_uniform_lane_with_one_lease_write_fewer() -> None:
    _probe("a", lease_seconds=60)
    _fire(3)

    with CaptureQueriesContext(connection) as queries:
        _relay()

    assert RAN == ["a", "a", "a"]
    assert _lease_writes(queries) == 0


def test_a_receiver_whose_lease_differs_from_the_lanes_is_extended() -> None:
    """A mixed lane is claimed under the setting's 330 s, more than either
    receiver's lease and its headroom, so each row is extended to its own as
    before: the band is what keeps a 60 s lane from holding 330 s."""
    _probe("a", lease_seconds=60)
    _probe("b", lease_seconds=90)
    _fire()

    with CaptureQueriesContext(connection) as queries:
        before = django_timezone.now()
        deliver_pending(lane="mail")

    assert _lease_writes(queries) == 2
    a, b = _rows()
    assert a.lease_expires_at - before < timedelta(seconds=61)
    assert b.lease_expires_at - before < timedelta(seconds=91)


def test_a_row_started_late_in_its_batch_is_extended() -> None:
    """The lower bound. A row whose claimed lease has fewer than its
    receiver's seconds left is extended, as a batch's tail is."""
    _probe("a", lease_seconds=60)
    _fire()
    now = django_timezone.now()
    # As a row ten seconds into its batch finds the lane's 66 s: 56 are left.
    claim_batch(worker_id="w", now=now, lease=timedelta(seconds=56), limit=10)

    with CaptureQueriesContext(connection) as queries:
        assert deliver_one(_rows()[0].pk, worker_id="w") is DeliveryStatus.SUCCEEDED

    assert _lease_writes(queries) == 1
    assert _rows()[0].lease_expires_at - now >= timedelta(seconds=60)


def test_a_row_claimed_on_a_later_clock_is_extended() -> None:
    """The upper bound. ``deliver_pending(now=...)`` claims on a moved clock
    and delivers on the real one, so the stamped lease is far more than the
    receiver needs; the extension sets it to that, on the real clock, as it
    always has."""
    _probe("a", lease_seconds=60)
    _fire()
    later = django_timezone.now() + timedelta(days=1)

    with CaptureQueriesContext(connection) as queries:
        before = django_timezone.now()
        deliver_pending(lane="mail", now=later)

    assert RAN == ["a"]
    assert _lease_writes(queries) == 1
    assert _rows()[0].lease_expires_at - before < timedelta(seconds=61)


@pytest.mark.parametrize(("back", "writes"), [(3, 0), (10, 1)])
def test_a_row_claimed_on_a_slightly_earlier_clock_starts_without_extending(
    back: int, writes: int
) -> None:
    """A clock moved back by less than a tenth of the lease leaves the stamped
    lease inside the band - at least the receiver's 60 s still ahead - so the
    row starts as it is. Moved back by more, it falls below and is extended."""
    _probe("a", lease_seconds=60)
    _fire()
    # Owed well before the moved clock, so the claim on it takes the row.
    DeliveryRecord.objects.update(available_at=django_timezone.now() - timedelta(hours=1))
    earlier = django_timezone.now() - timedelta(seconds=back)

    with CaptureQueriesContext(connection) as queries:
        deliver_pending(lane="mail", now=earlier)

    assert RAN == ["a"]
    assert _lease_writes(queries) == writes


def test_a_relay_on_a_later_clock_extends_on_the_real_one() -> None:
    _probe("a", lease_seconds=60)
    _fire()
    later = django_timezone.now() + timedelta(days=1)

    with CaptureQueriesContext(connection) as queries:
        before = django_timezone.now()
        _relay(now=lambda: later)

    assert RAN == ["a"]
    assert _lease_writes(queries) == 1
    assert _rows()[0].lease_expires_at - before < timedelta(seconds=61)


def test_a_row_another_worker_took_in_band_is_not_run() -> None:
    """The owner. The thief's lease is in this receiver's band, so only the
    owner on the row says the row is no longer this call's; the extension's
    compare-and-set then refuses it, as it always did."""
    _probe("a", lease_seconds=60)
    _fire()
    claim_batch(worker_id="A", now=django_timezone.now(), lease=timedelta(seconds=63), limit=10)
    DeliveryRecord.objects.update(claimed_by="B")

    assert deliver_one(_rows()[0].pk, worker_id="A") is None
    assert RAN == []
    assert _rows()[0].status == DeliveryStatus.CLAIMED


def test_a_claimed_row_with_no_lease_is_extended_and_delivered() -> None:
    """No claim writes a CLAIMED row without a lease, but one edited by hand
    still delivers rather than raising on the arithmetic."""
    _probe("a", lease_seconds=60)
    _fire()
    claim_batch(worker_id="w", now=django_timezone.now(), lease=timedelta(seconds=63), limit=10)
    DeliveryRecord.objects.update(lease_expires_at=None)

    with CaptureQueriesContext(connection) as queries:
        assert deliver_one(_rows()[0].pk, worker_id="w") is DeliveryStatus.SUCCEEDED

    assert _lease_writes(queries) == 1


def test_an_unclaimed_row_passed_directly_writes_no_lease() -> None:
    """The status. A PENDING row run directly is nobody's claim, so there is no
    lease to keep: it is run and acknowledged with no lease write."""
    _probe("a", lease_seconds=60)
    _fire()

    with CaptureQueriesContext(connection) as queries:
        assert deliver_one(_rows()[0].pk) is DeliveryStatus.SUCCEEDED

    assert _lease_writes(queries) == 0


class _RelayDied(BaseException):
    """Escapes the relay's own survival, as a killed process does."""


def test_a_dead_relays_unstarted_rows_are_claimable_after_their_lanes_lease() -> None:
    """A uniform lane recovers in its own lease and the headroom, 66 s here,
    not in ``LEASE_SECONDS``."""
    _probe("a", lease_seconds=60)
    _fire(2)
    claimed = django_timezone.now()

    def die(*args: object, **kwargs: object) -> None:
        raise _RelayDied

    with mock.patch.object(run_relay_module, "_deliver_or_survive", die), pytest.raises(_RelayDied):
        _relay(now=lambda: claimed)

    def take(at: datetime) -> list[int]:
        return claim_batch(
            worker_id="next", now=at, lease=timedelta(seconds=66), limit=10, lane="mail"
        )

    assert take(claimed + timedelta(seconds=65)) == []
    assert len(take(claimed + timedelta(seconds=67))) == 2


def test_a_long_lease_lane_recovers_in_the_setting() -> None:
    """A lane of receivers declaring 1800 s is claimed under ``LEASE_SECONDS``
    and its headroom, 330 s, as the default lane is: a killed relay's
    unstarted tail is not held for 1980 s."""
    _probe("a", lease_seconds=1800)
    _fire(2)
    claimed = django_timezone.now()

    def die(*args: object, **kwargs: object) -> None:
        raise _RelayDied

    with mock.patch.object(run_relay_module, "_deliver_or_survive", die), pytest.raises(_RelayDied):
        _relay(now=lambda: claimed)

    def take(at: datetime) -> list[int]:
        return claim_batch(
            worker_id="next", now=at, lease=timedelta(seconds=330), limit=10, lane="mail"
        )

    assert take(claimed + timedelta(seconds=329)) == []
    assert len(take(claimed + timedelta(seconds=331))) == 2


def test_an_eager_attempt_for_long_lease_receivers_recovers_in_the_setting() -> None:
    """The same cap on the attempt after commit, when every eager receiver
    shares one lease longer than ``LEASE_SECONDS``."""
    _probe("eager", eager=True, lease_seconds=1800)

    def die(*args: object, **kwargs: object) -> None:
        raise _RelayDied

    with (
        receiver_deleted("testapp.eager"),
        _claimed_leases(claim_module) as leases,
        mock.patch.object(deliver_module, "dispatch_one", die),
        pytest.raises(_RelayDied),
    ):
        _fire()

    assert leases == [timedelta(seconds=330)]
    row = _rows()[0]
    assert row.status == DeliveryStatus.CLAIMED

    def take(at: datetime) -> list[int]:
        return claim_batch(worker_id="next", now=at, lease=timedelta(seconds=330), limit=10)

    assert take(row.claimed_at + timedelta(seconds=329)) == []
    assert take(row.claimed_at + timedelta(seconds=331)) == [row.pk]


def test_a_batch_tail_another_relay_took_is_not_sent_twice(caplog: Any) -> None:
    """A uniform lane's batch that outlasts its 66 s: a second relay takes the
    tail, and the first, reaching it, finds the row gone and logs it."""
    _probe("a", lease_seconds=60)
    _fire(2)
    claimed = django_timezone.now() - timedelta(seconds=67)
    # The first relay claims both, sixty-seven seconds ago, and delivers
    # neither yet: the claim is all that is wanted of it here.
    with (
        _claimed_leases(run_relay_module) as leases,
        mock.patch.object(run_relay_module, "_deliver_or_survive", lambda *a, **k: None),
    ):
        _relay(now=lambda: claimed)
    assert leases == [timedelta(seconds=66)]
    first, tail = _rows()
    # The second relay takes the lapsed rows as its own claim would.
    taken = claim_batch(
        worker_id="second", now=django_timezone.now(), lease=leases[0], limit=10, lane="mail"
    )
    assert taken == [first.pk, tail.pk]

    with caplog.at_level("WARNING", logger="django_domain_events.delivery.deliver"):
        assert deliver_one(tail.pk, worker_id="relay") is None

    assert RAN == []
    assert any("lost delivery" in r.getMessage() for r in caplog.records)
    assert deliver_one(tail.pk, worker_id="second") is DeliveryStatus.SUCCEEDED
    assert RAN == ["a"]


# The task site.


@pytest.fixture
def task_backend(settings) -> Iterator[Any]:
    def configure(backend: type) -> None:
        settings.DJANGO_DOMAIN_EVENTS = {"TASK_BACKEND": f"{__name__}.{backend.__name__}"}

    yield configure


def test_a_hand_off_right_after_the_claim_writes_no_lease(task_backend: Any) -> None:
    task_backend(RecordingBackend)
    _probe("task", site="task", lease_seconds=60)
    _fire()

    with CaptureQueriesContext(connection) as queries:
        deliver_pending(lane="mail")

    assert len(MESSAGES) == 1
    assert _lease_writes(queries) == 0


def test_a_worker_taking_the_row_within_the_band_writes_no_lease(task_backend: Any) -> None:
    """The take moves the owner and leaves the lease, so a worker that took the
    row soon after the hand-off finds it already leased for the receiver."""
    task_backend(RecordingBackend)
    _probe("task", site="task", lease_seconds=60)
    _fire()
    deliver_pending(lane="mail")
    (args, kwargs) = MESSAGES[0]

    with CaptureQueriesContext(connection) as queries:
        deliver_delivery(*args, **kwargs)

    assert RAN == ["task"]
    assert _lease_writes(queries) == 0


def test_a_task_run_inside_the_hand_off_writes_no_lease(task_backend: Any) -> None:
    task_backend(InlineBackend)
    _probe("task", site="task", lease_seconds=60)
    _fire(2)

    with CaptureQueriesContext(connection) as queries:
        deliver_pending(lane="mail")

    assert RAN == ["task", "task"]
    assert _lease_writes(queries) == 0


def test_a_worker_taking_the_row_after_a_slow_queue_extends(task_backend: Any) -> None:
    """Ten seconds in the queue leave 56 of the 66 s: below the receiver's 60,
    so the worker extends, as it did after every take before."""
    task_backend(RecordingBackend)
    _probe("task", site="task", lease_seconds=60)
    _fire()
    deliver_pending(lane="mail")
    (args, kwargs) = MESSAGES[0]
    # The queue's ten seconds, moved onto the lease rather than waited out.
    row = _rows()[0]
    DeliveryRecord.objects.update(lease_expires_at=row.lease_expires_at - timedelta(seconds=10))

    with CaptureQueriesContext(connection) as queries:
        deliver_delivery(*args, **kwargs)

    assert RAN == ["task"]
    assert _lease_writes(queries) == 1


def test_a_row_another_worker_took_in_band_is_not_enqueued(task_backend: Any) -> None:
    """The owner, at the hand-off: a message carrying this worker's claim for a
    row the thief holds would be refused by the take, but it must not be sent."""
    task_backend(RecordingBackend)
    _probe("task", site="task", lease_seconds=60)
    _fire()
    claim_batch(worker_id="A", now=django_timezone.now(), lease=timedelta(seconds=63), limit=10)
    DeliveryRecord.objects.update(claimed_by="B")

    assert dispatch_one(_rows()[0].pk, worker_id="A") is None
    assert MESSAGES == []


def test_a_hand_off_on_a_later_clock_extends_on_the_real_one(task_backend: Any) -> None:
    task_backend(RecordingBackend)
    _probe("task", site="task", lease_seconds=60)
    _fire()
    later = django_timezone.now() + timedelta(days=1)

    with CaptureQueriesContext(connection) as queries:
        before = django_timezone.now()
        deliver_pending(lane="mail", now=later)

    assert len(MESSAGES) == 1
    assert _lease_writes(queries) == 1
    assert _rows()[0].lease_expires_at - before < timedelta(seconds=61)


# USE_TZ = False.


def test_the_band_decides_the_same_on_naive_time(settings) -> None:
    """The band compares the row's lease with the clock ``_deliver_one`` reads,
    both naive under the setting; an aware clock would raise ``TypeError``."""
    settings.USE_TZ = False
    _probe("a", lease_seconds=60)
    _fire(2)
    with CaptureQueriesContext(connection) as queries:
        assert deliver_pending(lane="mail") == {DeliveryStatus.SUCCEEDED: 2}
    assert _lease_writes(queries) == 0

    # And a late row is still extended.
    _fire()
    claim_batch(worker_id="w", now=django_timezone.now(), lease=timedelta(seconds=56), limit=10)
    late = DeliveryRecord.objects.get(status=DeliveryStatus.CLAIMED)
    assert django_timezone.is_naive(late.lease_expires_at)
    with CaptureQueriesContext(connection) as queries:
        assert deliver_one(late.pk, worker_id="w") is DeliveryStatus.SUCCEEDED
    assert _lease_writes(queries) == 1
