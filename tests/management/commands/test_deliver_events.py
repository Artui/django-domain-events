"""Tests mirroring ``django_domain_events/management/commands/deliver_events.py``."""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connection, transaction

from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.fire import fire
from django_domain_events.management.commands import deliver_events
from tests.conftest import receiver_replaced
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_reports_what_it_delivered(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    out = StringIO()
    call_command("deliver_events", "--once", stdout=out)
    assert "succeeded: 2" in out.getvalue()


def test_nothing_owed_says_so() -> None:
    out = StringIO()
    call_command("deliver_events", "--once", stdout=out)
    assert out.getvalue().strip() == "Nothing owed."


def test_a_limit_is_passed_through(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    out = StringIO()
    call_command("deliver_events", "--once", "--limit", "1", stdout=out)
    assert "succeeded: 1" in out.getvalue()


def test_the_relay_refuses_where_locks_cannot_be_skipped() -> None:
    """SQLite cannot express a skipped lock, so two relays on it would hand the
    same row to two receivers on every pass.

    Guarded on the backend rather than left to run everywhere. Without ``--passes``
    the command relays forever, so on a backend that does *not* refuse this call
    never returns -- and the version of this test that did not skip hung the whole
    Postgres suite until it was killed. A test that passes only because the
    default backend refuses is a test that passes for the wrong reason.
    """
    if connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend supports skipped locks, so the relay would run")
    with pytest.raises(RuntimeError, match="SKIP LOCKED"):
        call_command("deliver_events")


def test_the_relay_runs_for_a_bounded_number_of_passes(
    order: OrderPlaced, record: list[str]
) -> None:
    """The relay path on a backend that supports it, kept finite by --passes."""
    if not connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend cannot skip locks, so the relay refuses")
    with transaction.atomic():
        fire(order)
    out = StringIO()
    call_command("deliver_events", "--passes", "1", stdout=out)
    assert "succeeded: 2" in out.getvalue()


# --- signals ------------------------------------------------------------------
#
# The relay itself refuses to start on SQLite, which is the coverage gate, so
# these replace it with a stand-in that raises the signal and reports what the
# stop it was handed says. The test on a real relay is further down and runs
# where the relay does.

STOPPING = (signal.SIGTERM, signal.SIGINT)


def _handlers() -> dict[int, object]:
    return {signum: signal.getsignal(signum) for signum in STOPPING}


@pytest.mark.parametrize("signum", STOPPING, ids=["SIGTERM", "SIGINT"])
def test_a_signal_asks_the_relay_to_stop(
    signum: signal.Signals, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first signal only sets the flag the relay reads, so the delivery in
    hand finishes and the rest of the batch is handed back."""
    seen: list[bool] = []

    def relay(*, worker_id: str, passes: int | None, stop: Callable[[], bool]) -> dict:
        seen.append(stop())
        signal.raise_signal(signum)
        seen.append(stop())
        return {}

    monkeypatch.setattr(deliver_events, "run_relay", relay)
    before = _handlers()

    call_command("deliver_events", stdout=StringIO())

    assert seen == [False, True]
    assert _handlers() == before, "the previous handlers were not restored"


def test_a_second_signal_exits_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """An operator who sends it twice has stopped waiting for the receiver in
    hand. A BaseException, because the relay swallows every Exception a
    delivery raises and a receiver may too."""

    def relay(*, worker_id: str, passes: int | None, stop: Callable[[], bool]) -> dict:
        signal.raise_signal(signal.SIGTERM)
        signal.raise_signal(signal.SIGTERM)
        raise AssertionError("the second signal did not end the command")

    monkeypatch.setattr(deliver_events, "run_relay", relay)
    before = _handlers()

    with pytest.raises(SystemExit) as exited:
        call_command("deliver_events", stdout=StringIO())

    assert exited.value.code == 128 + signal.SIGTERM
    assert _handlers() == before, "the previous handlers were not restored on exit"


def test_off_the_main_thread_the_relay_runs_without_a_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Python delivers signals to the main thread only and refuses to install a
    handler from any other, so a relay started from a thread cannot be stopped
    by one. It still runs, and says so, rather than refusing."""
    seen: list[bool] = []

    def relay(*, worker_id: str, passes: int | None, stop: Callable[[], bool]) -> dict:
        seen.append(stop())
        return {}

    monkeypatch.setattr(deliver_events, "run_relay", relay)
    before = _handlers()
    err = StringIO()
    failures: list[BaseException] = []

    def run() -> None:
        try:
            call_command("deliver_events", stdout=StringIO(), stderr=err)
        except BaseException as exc:  # reported on the test's own thread below
            failures.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()

    assert failures == []
    assert seen == [False]
    assert "not on the main thread" in err.getvalue()
    assert _handlers() == before


def test_a_signal_mid_batch_hands_back_the_rest(order: OrderPlaced, record: list[str]) -> None:
    """The whole path on a relay that runs: SIGTERM arrives while a receiver is
    running, that delivery finishes, and every other row of the batch is
    claimable by another worker at once.

    Bounded by ``--passes`` so a stop that is not honoured fails the assertions
    below rather than relaying forever.
    """
    if not connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend cannot skip locks, so the relay refuses")
    with transaction.atomic():
        fire(order)
        fire(order)

    def interrupted(evt: OrderPlaced) -> None:
        signal.raise_signal(signal.SIGTERM)

    out = StringIO()
    with receiver_replaced("testapp.durable_receiver", interrupted):
        call_command("deliver_events", "--passes", "3", stdout=out)

    assert "succeeded: 1" in out.getvalue(), "the delivery in hand was not finished"
    taken = claim_batch(
        worker_id="w2", now=datetime.now(timezone.utc), lease=timedelta(minutes=5), limit=10
    )
    assert len(taken) == 3, "the rest of the batch was not handed back"
