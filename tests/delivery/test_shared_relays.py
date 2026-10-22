"""A relay serving several lanes beside a relay serving one of them.

Moving a lane between a shared relay and one of its own is a deployment
change, so during a rollout both run against the same rows. These hold that
the two never take the same row and that a row the stopping one held is the
other's at once. Each relay runs on a thread of its own with its own
connection, as ``test_concurrent_claims.py`` does: skipped locks are about
what a second session sees while the first holds a row lock, so these mean
something only on a backend that has them.
"""

from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
from django.db import connection, connections, transaction

from django_domain_events.delivery import run_relay as run_relay_module
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import event_registered, receiver_registered

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(
        not connection.features.has_select_for_update_skip_locked,
        reason="skipped locks are what this asserts, so it needs a backend with them",
    ),
]

DELIVERED: list[tuple[str, str, int]] = []
"""Each delivery as (the relay's thread, the lane, the event's value)."""


@dataclass(frozen=True, slots=True)
class SharedPush:
    value: int


@dataclass(frozen=True, slots=True)
class SharedSms:
    value: int


def _lane_receiver(lane: str, event_class: type, lease_seconds: int) -> RegisteredReceiver:
    return RegisteredReceiver(
        key=f"tests.shared_{lane}",
        event_class=event_class,
        func=lambda evt: DELIVERED.append((threading.current_thread().name, lane, evt.value)),
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane=lane,
        lease_seconds=lease_seconds,
    )


@pytest.fixture
def lanes() -> Iterator[None]:
    DELIVERED.clear()
    with (
        event_registered(SharedPush, "tests.SharedPush"),
        event_registered(SharedSms, "tests.SharedSms"),
        receiver_registered(_lane_receiver("push", SharedPush, lease_seconds=60)),
        receiver_registered(_lane_receiver("sms", SharedSms, lease_seconds=30)),
    ):
        yield
    DELIVERED.clear()


def _fire(event_class: type, count: int) -> None:
    with transaction.atomic():
        for value in range(count):
            fire(event_class(value=value))


def _held_at_the_first_sms_claim(barrier: threading.Barrier) -> Callable[..., Any]:
    """An execute wrapper that, once its SMS claim has selected its rows, waits
    for the other relay's to have done the same before going on.

    Inside the claim's transaction, after the select and before the update:
    so the two claims have both chosen their rows before either has written
    them, which is the moment a missing lock shows. Recognised as the select
    ordered by ``available_at`` with the SMS receiver's key among its
    parameters.
    """
    waited: list[bool] = []

    def wrapper(
        execute: Callable[..., Any], sql: str, params: Any, many: bool, context: Any
    ) -> Any:
        result = execute(sql, params, many, context)
        is_sms_claim = (
            sql.startswith("SELECT")
            and '"available_at"' in sql.partition("ORDER BY")[2]
            and "tests.shared_sms" in (params or ())
        )
        if is_sms_claim and not waited:
            waited.append(True)
            barrier.wait(timeout=10)
        return result

    return wrapper


def _on_a_thread(
    name: str,
    relay: Callable[[], object],
    errors: list[BaseException],
    barrier: threading.Barrier | None = None,
) -> threading.Thread:
    """Run ``relay`` on a thread of its own, which opens its own connection and
    closes it when done."""

    def body() -> None:
        try:
            if barrier is None:
                relay()
            else:
                with connections[write_alias()].execute_wrapper(
                    _held_at_the_first_sms_claim(barrier)
                ):
                    relay()
        except BaseException as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    return threading.Thread(target=body, name=name)


def test_a_shared_relay_and_a_lanes_own_relay_never_take_the_same_row(
    lanes: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each relay's first SMS claim, having selected its rows, waits on a
    barrier for the other's, so the two claims contend for the same rows
    rather than happening to miss each other. Every row is delivered once, no
    row is in both relays' claims, and every SMS claim, whichever relay made
    it, stamps SMS's own lease."""
    _fire(SharedPush, 20)
    _fire(SharedSms, 40)
    barrier = threading.Barrier(2)
    claims: list[tuple[str, str, timedelta, list[int]]] = []
    real = run_relay_module.claim_batch

    def recording(**kwargs: Any) -> list[int]:
        ids = real(**kwargs)
        claims.append((kwargs["worker_id"], kwargs["lane"], kwargs["lease"], ids))
        return ids

    monkeypatch.setattr(run_relay_module, "claim_batch", recording)
    errors: list[BaseException] = []
    threads = [
        _on_a_thread(
            "shared",
            lambda: run_relay(
                worker_id="shared",
                lane=["push", "sms"],
                batch_size=5,
                passes=12,
                wait=lambda _: False,
            ),
            errors,
            barrier,
        ),
        _on_a_thread(
            "own",
            lambda: run_relay(
                worker_id="own", lane="sms", batch_size=5, passes=12, wait=lambda _: False
            ),
            errors,
            barrier,
        ),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == []
    assert not any(t.is_alive() for t in threads), "a relay did not finish"
    delivered = Counter((lane, value) for _, lane, value in DELIVERED)
    assert len(delivered) == 60, "a row was never delivered"
    assert set(delivered.values()) == {1}, "a row was delivered twice"
    sms_by_worker = {
        worker: {pk for w, lane, _, ids in claims if w == worker and lane == "sms" for pk in ids}
        for worker in ("shared", "own")
    }
    assert sms_by_worker["shared"] and sms_by_worker["own"], "the relays did not contend"
    assert not sms_by_worker["shared"] & sms_by_worker["own"], "both relays claimed one row"
    assert {lease for _, lane, lease, _ in claims if lane == "sms"} == {timedelta(seconds=33)}
    assert not DeliveryRecord.objects.exclude(status=DeliveryStatus.SUCCEEDED).exists()


def test_a_row_a_stopping_shared_relay_held_is_the_other_relays_at_once(lanes: None) -> None:
    """The shared relay is told to stop after the first row of its SMS batch,
    and holds the rest until the SMS relay has found nothing it may claim and
    gone idle. It then hands them back, and the SMS relay claims them on its
    very next pass rather than when the 33-second lease lapses."""
    _fire(SharedSms, 5)
    holding = threading.Event()
    own_idle = threading.Event()
    done = threading.Event()

    def stop_once_holding() -> bool:
        if not DELIVERED:
            return False
        holding.set()
        own_idle.wait(timeout=10)
        return True

    def shared() -> None:
        try:
            run_relay(
                worker_id="shared",
                lane=["push", "sms"],
                stop=stop_once_holding,
                wait=lambda _: False,
                passes=1,
            )
        finally:
            done.set()

    def idle(timeout: float) -> bool:
        own_idle.set()
        return done.wait(timeout=10)

    def own() -> None:
        assert holding.wait(timeout=10)
        run_relay(worker_id="own", lane="sms", passes=2, wait=idle)

    errors: list[BaseException] = []
    threads = [_on_a_thread("shared", shared, errors), _on_a_thread("own", own, errors)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == []
    assert own_idle.is_set(), "the SMS relay found something to claim while the rows were held"
    by_relay = Counter(relay for relay, _, _ in DELIVERED)
    assert by_relay == {"shared": 1, "own": 4}, "the handed-back rows waited for their lease"
