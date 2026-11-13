from __future__ import annotations

import re
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.db import connection, transaction

from django_domain_events.delivery.claim_batch import claim_batch
from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.delivery.drain_outbox import drain_outbox
from django_domain_events.delivery.fire import fire
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.management.commands import deliver_events
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import event_registered, receiver_registered, receiver_replaced
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


def _captured(monkeypatch: pytest.MonkeyPatch, name: str) -> list[dict[str, object]]:
    """Replace the relay or the single pass with one that records its arguments."""
    seen: list[dict[str, object]] = []

    def stand_in(**kwargs: object) -> dict:
        seen.append(kwargs)
        return {}

    monkeypatch.setattr(deliver_events, name, stand_in)
    return seen


@pytest.mark.parametrize(
    ("argv", "name"),
    [([], "run_relay"), (["--once"], "deliver_pending")],
    ids=["relay", "once"],
)
def test_without_a_lane_it_serves_the_default_lane(
    argv: list[str], name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both modes: a cron running --once stays out of the mail lane."""
    seen = _captured(monkeypatch, name)

    call_command("deliver_events", *argv, stdout=StringIO())

    assert (seen[0]["lane"], seen[0]["batch_size"]) == ("default", None)
    assert seen[0]["lane_batch_sizes"] is None


@pytest.mark.parametrize(
    ("argv", "name"),
    [([], "run_relay"), (["--once"], "deliver_pending")],
    ids=["relay", "once"],
)
def test_a_lane_and_a_batch_size_are_passed_through(
    argv: list[str], name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _captured(monkeypatch, name)

    call_command("deliver_events", *argv, "--lane", "mail", "--batch-size", "2", stdout=StringIO())

    assert (seen[0]["lane"], seen[0]["batch_size"]) == ("mail", 2)


@pytest.mark.parametrize(
    ("argv", "name"),
    [([], "run_relay"), (["--once"], "deliver_pending")],
    ids=["relay", "once"],
)
def test_one_lane_alone_is_passed_as_that_lane(
    argv: list[str], name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--lane mail`` serves ``mail`` and nothing else."""
    seen = _captured(monkeypatch, name)

    call_command("deliver_events", *argv, "--lane", "mail", stdout=StringIO())

    assert seen[0]["lane"] == "mail"


@pytest.mark.parametrize(
    ("argv", "name"),
    [([], "run_relay"), (["--once"], "deliver_pending")],
    ids=["relay", "once"],
)
def test_a_repeated_lane_serves_every_lane_named(
    argv: list[str], name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _captured(monkeypatch, name)

    call_command("deliver_events", *argv, "--lane", "sms", "--lane", "push", stdout=StringIO())

    assert seen[0]["lane"] == ["sms", "push"]


def test_a_single_pass_over_two_lanes_delivers_both(order: OrderPlaced, record: list[str]) -> None:
    """The real single pass, with no stand-in, over the default lane and a lane of its own."""
    mail = RegisteredReceiver(
        key="tests.mail",
        event_class=OrderPlaced,
        func=lambda evt: None,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane="mail",
    )
    with receiver_registered(mail):
        with transaction.atomic():
            fire(order)
        out = StringIO()
        call_command("deliver_events", "--once", "--lane", "default", "--lane", "mail", stdout=out)

    assert out.getvalue().strip() == "succeeded: 3"


@pytest.mark.parametrize(
    ("argv", "name"),
    [([], "run_relay"), (["--once"], "deliver_pending")],
    ids=["relay", "once"],
)
def test_a_lane_batch_size_is_split_at_its_last_equals_sign(
    argv: list[str], name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lane name may contain ``=`` and a size cannot, so ``a=b=5`` sizes the lane ``a=b``."""
    seen = _captured(monkeypatch, name)

    call_command(
        "deliver_events",
        *argv,
        "--lane-batch-size",
        "a=b=5",
        "--lane-batch-size",
        "mail=2",
        stdout=StringIO(),
    )

    assert seen[0]["lane_batch_sizes"] == {"a=b": 5, "mail": 2}


@pytest.mark.parametrize(
    ("values", "message"),
    [
        (["mail"], "--lane-batch-size takes LANE=N, got 'mail'"),
        (["=5"], "--lane-batch-size '=5' names no lane"),
        (
            ["mail=five"],
            "--lane-batch-size 'mail=five' sizes lane 'mail' with 'five', not an integer",
        ),
        (["mail=2", "mail=2"], "--lane-batch-size sizes lane 'mail' twice"),
    ],
    ids=["no-equals", "no-lane", "not-an-integer", "twice"],
)
def test_a_malformed_lane_batch_size_is_refused(
    values: list[str], message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Before anything runs."""
    seen = _captured(monkeypatch, "run_relay")
    argv = [arg for value in values for arg in ("--lane-batch-size", value)]

    with pytest.raises(CommandError, match=re.escape(message)):
        call_command("deliver_events", *argv, stdout=StringIO())
    assert seen == []


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("default=0", "lane_batch_sizes['default'] must be positive"),
        ("mail=2", "lane_batch_sizes names lane 'mail', which is not served here"),
    ],
    ids=["below-one", "not-served"],
)
def test_a_lane_size_the_pass_cannot_use_is_refused_as_python_refuses_it(
    value: str, message: str
) -> None:
    """Left to deliver_pending, so the command line and Python refuse the same errors."""
    with pytest.raises(ValueError, match=re.escape(message)):
        call_command("deliver_events", "--once", "--lane-batch-size", value, stdout=StringIO())


def test_a_single_pass_with_a_batch_size_delivers(order: OrderPlaced, record: list[str]) -> None:
    """A --batch-size alone is not mistaken for the refused limit-and-batch pair."""
    with transaction.atomic():
        fire(order)
    out = StringIO()
    call_command("deliver_events", "--once", "--batch-size", "1", stdout=out)
    assert "succeeded: 2" in out.getvalue()


def test_a_single_pass_refuses_a_limit_with_a_batch_size() -> None:
    with pytest.raises(ValueError, match="limit=3 sizes each claim as its lane.s share of it"):
        call_command("deliver_events", "--once", "--limit", "3", "--batch-size", "2")


@pytest.mark.parametrize("limit", ["5", "0"])
def test_the_relay_refuses_a_limit(monkeypatch: pytest.MonkeyPatch, limit: str) -> None:
    """Before the relay starts, so the refusal holds on every backend."""
    seen = _captured(monkeypatch, "run_relay")
    with pytest.raises(CommandError, match="--limit caps a single pass and needs --once"):
        call_command("deliver_events", "--limit", limit, "--passes", "1")
    assert seen == []


def test_the_relay_refuses_where_locks_cannot_be_skipped() -> None:
    """SQLite cannot skip locks, so two relays would double-deliver rows."""
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
    """The first signal sets a flag: the delivery in hand finishes, the rest returns."""
    seen: list[bool] = []

    def relay(*, worker_id: str, passes: int | None, stop: Callable[[], bool], **_: object) -> dict:
        seen.append(stop())
        signal.raise_signal(signum)
        seen.append(stop())
        return {}

    monkeypatch.setattr(deliver_events, "run_relay", relay)
    before = _handlers()

    call_command("deliver_events", stdout=StringIO())

    assert seen == [False, True]
    assert _handlers() == before, "the previous handlers were not restored"


def test_a_single_pass_leaves_the_signals_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    """--once has no batch to hand back: a signal ends it like any command."""
    during: list[dict[int, object]] = []

    def one_pass(*, limit: int | None, worker_id: str, **_: object) -> dict:
        during.append(_handlers())
        return {}

    monkeypatch.setattr(deliver_events, "deliver_pending", one_pass)
    before = _handlers()

    call_command("deliver_events", "--once", stdout=StringIO())

    assert during == [before]


def test_a_second_signal_exits_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """An operator who sends it twice has stopped waiting for the receiver in hand."""

    def relay(*, worker_id: str, passes: int | None, stop: Callable[[], bool], **_: object) -> dict:
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
    """Signals reach only the main thread, so a thread-started relay ignores them."""
    seen: list[bool] = []

    def relay(*, worker_id: str, passes: int | None, stop: Callable[[], bool], **_: object) -> dict:
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
    """SIGTERM mid-receiver: that delivery finishes, the other rows are claimable."""
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


_steps: list[tuple[str, object]] = []


def startup(**kwargs: object) -> None:
    _steps.append(("startup", kwargs))


def failing_startup(**kwargs: object) -> None:
    raise RuntimeError("flags did not load")


@pytest.fixture
def steps() -> Iterator[list[tuple[str, object]]]:
    _steps.clear()
    yield _steps
    _steps.clear()


@pytest.mark.parametrize(
    ("argv", "name"),
    [(["--passes", "1"], "run_relay"), (["--once"], "deliver_pending")],
    ids=["relay", "once"],
)
def test_relay_startup_runs_once_before_the_first_claim(
    argv: list[str],
    name: str,
    monkeypatch: pytest.MonkeyPatch,
    settings,
    steps: list[tuple[str, object]],
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.startup"}

    def stand_in(**kwargs: object) -> dict:
        steps.append(("claim", None))
        return {}

    monkeypatch.setattr(deliver_events, name, stand_in)

    call_command(
        "deliver_events",
        *argv,
        "--worker-id",
        "box-1",
        "--lane",
        "a",
        "--lane",
        "b",
        stdout=StringIO(),
    )

    assert steps == [("startup", {"worker_id": "box-1", "lanes": ["a", "b"]}), ("claim", None)]


def test_relay_startup_is_handed_the_default_lane_and_worker_id(
    monkeypatch: pytest.MonkeyPatch, settings, steps: list[tuple[str, object]]
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.startup"}
    _captured(monkeypatch, "deliver_pending")

    call_command("deliver_events", "--once", stdout=StringIO())

    kwargs = steps[0][1]
    assert isinstance(kwargs, dict)
    assert kwargs["lanes"] == ["default"]
    assert re.fullmatch(r".+:\d+", str(kwargs["worker_id"]))


def test_an_exception_from_relay_startup_stops_the_command_before_any_claim(
    order: OrderPlaced, settings
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.failing_startup"}
    with transaction.atomic():
        fire(order)

    with pytest.raises(RuntimeError, match="flags did not load"):
        call_command("deliver_events", "--once", stdout=StringIO())

    rows = set(DeliveryRecord.objects.values_list("claimed_by", "attempts"))
    assert rows == {("", 0)}, "the command claimed rows anyway"


def test_relay_startup_does_not_run_for_an_invalid_command(
    settings, steps: list[tuple[str, object]]
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.startup"}

    with pytest.raises(CommandError):
        call_command("deliver_events", "--limit", "1")

    assert steps == []


def test_relay_startup_runs_before_an_unknown_lane_is_refused(
    settings, steps: list[tuple[str, object]]
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.startup"}

    with pytest.raises(ValueError, match="nosuch"):
        call_command("deliver_events", "--once", "--lane", "nosuch", stdout=StringIO())

    assert [step for step, _ in steps] == ["startup"]


def test_the_delivery_functions_called_from_python_never_call_relay_startup(
    order: OrderPlaced, record: list[str], settings, steps: list[tuple[str, object]]
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.startup"}
    with transaction.atomic():
        fire(order)

    deliver_pending()
    drain_outbox()
    run_relay(worker_id="w1", passes=1, wait=lambda _: False, allow_unsafe_concurrency=True)

    assert steps == []
    assert record, "nothing was delivered, so the functions proved nothing"


def test_relay_startup_runs_before_a_relay_without_skip_locked_is_refused(
    settings, steps: list[tuple[str, object]]
) -> None:
    if connection.vendor != "sqlite":
        pytest.skip("only a database without SKIP LOCKED refuses the relay")
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.startup"}

    with pytest.raises(RuntimeError, match="SKIP LOCKED"):
        call_command("deliver_events", "--passes", "1", stdout=StringIO())

    assert [step for step, _ in steps] == ["startup"]


def test_once_holds_off_a_lane_an_operator_paused_until_resume_lane(
    order: OrderPlaced, record: list[str]
) -> None:
    with transaction.atomic():
        fire(order)
    call_command("pause_lane", "default", "--for", "600", stdout=StringIO())

    paused = StringIO()
    call_command("deliver_events", "--once", stdout=paused)
    call_command("resume_lane", "default", stdout=StringIO())
    resumed = StringIO()
    call_command("deliver_events", "--once", stdout=resumed)

    assert paused.getvalue().strip() == "Nothing owed."
    assert "succeeded: 2" in resumed.getvalue()


def test_a_relay_without_threads_is_handed_none_for_both(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _captured(monkeypatch, "run_relay")

    call_command("deliver_events", stdout=StringIO())

    assert (seen[0]["lane_concurrency"], seen[0]["lane_rates"]) == (None, None)


def test_lane_concurrency_and_rate_are_passed_to_the_relay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _captured(monkeypatch, "run_relay")

    call_command(
        "deliver_events",
        "--lane",
        "mail",
        "--lane-concurrency",
        "mail=6",
        "--lane-rate",
        "mail=0.5",
        stdout=StringIO(),
    )

    assert (seen[0]["lane"], seen[0]["lane_concurrency"], seen[0]["lane_rates"]) == (
        "mail",
        {"mail": 6},
        {"mail": 0.5},
    )


def test_a_lane_named_twice_still_takes_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """The lanes are a set, as ``run_relay`` reads them, so a repeat is not a second lane."""
    seen = _captured(monkeypatch, "run_relay")

    call_command(
        "deliver_events", "--lane", "mail", "--lane", "mail", "--lane-concurrency", "mail=3"
    )

    assert seen[0]["lane_concurrency"] == {"mail": 3}


@pytest.mark.parametrize(
    "flag",
    [["--lane-concurrency", "default=1"], ["--lane-rate", "default=2"]],
    ids=["concurrency", "rate"],
)
def test_once_refuses_lane_concurrency_and_rate(
    flag: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _captured(monkeypatch, "deliver_pending")

    with pytest.raises(CommandError, match="--once delivers one row at a time without them"):
        call_command("deliver_events", "--once", *flag)

    assert seen == []


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["--lane-concurrency", "mail"], "--lane-concurrency takes LANE=N, got 'mail'"),
        (["--lane-rate", "mail"], "--lane-rate takes LANE=R, got 'mail'"),
        (["--lane-rate", "=5"], "--lane-rate '=5' names no lane"),
        (
            ["--lane-concurrency", "mail=2.5"],
            "--lane-concurrency 'mail=2.5' sizes lane 'mail' with '2.5', not an integer",
        ),
        (
            ["--lane-rate", "mail=fast"],
            "--lane-rate 'mail=fast' rates lane 'mail' with 'fast', not a number",
        ),
        (["--lane-rate", "mail=1", "--lane-rate", "mail=2"], "--lane-rate rates lane 'mail' twice"),
    ],
    ids=["no-equals", "rate-no-equals", "no-lane", "not-an-integer", "not-a-number", "twice"],
)
def test_a_malformed_lane_concurrency_or_rate_is_refused(
    argv: list[str], message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _captured(monkeypatch, "run_relay")

    with pytest.raises(CommandError, match=re.escape(message)):
        call_command("deliver_events", "--lane", "mail", *argv)

    assert seen == []


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (
            ["--lane", "default", "--lane", "mail", "--lane-concurrency", "mail=4"],
            "Lane 'mail' has worker threads or a rate, which a relay serving several lanes",
        ),
        (
            ["--lane", "default", "--lane", "mail", "--lane-rate", "mail=0.5"],
            "Lane 'mail' has worker threads or a rate, which a relay serving several lanes",
        ),
        (
            ["--lane", "mail", "--lane-concurrency", "mail=4", "--batch-size", "5"],
            "drop batch_size and lane_batch_sizes",
        ),
        (
            ["--lane", "mail", "--lane-rate", "mail=2", "--lane-batch-size", "mail=5"],
            "drop batch_size and lane_batch_sizes",
        ),
        (
            ["--lane", "mail", "--lane-concurrency", "push=4"],
            "lane_concurrency names lane 'push', which is not served here",
        ),
        (
            ["--lane", "mail", "--lane-concurrency", "mail=0"],
            "lane_concurrency['mail'] must be a positive int, got 0",
        ),
        (
            ["--lane", "mail", "--lane-rate", "mail=nan"],
            "lane_rates['mail'] must be a number above 0, got nan",
        ),
    ],
    ids=[
        "several-lanes",
        "several-lanes-rate",
        "batch-size",
        "lane-batch-size",
        "not-served",
        "no-workers",
        "nan-rate",
    ],
)
def test_threads_the_relay_refuses_are_a_command_line_error_before_startup(
    argv: list[str],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    settings,
    steps: list[tuple[str, object]],
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"RELAY_STARTUP": f"{__name__}.startup"}
    seen = _captured(monkeypatch, "run_relay")

    with pytest.raises(CommandError, match=re.escape(message)):
        call_command("deliver_events", *argv)

    assert (seen, steps) == ([], [])


def test_one_worker_and_no_rate_keeps_a_batch_size_and_several_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _captured(monkeypatch, "run_relay")

    call_command(
        "deliver_events",
        "--lane",
        "default",
        "--lane",
        "mail",
        "--lane-concurrency",
        "mail=1",
        "--batch-size",
        "5",
    )

    assert (seen[0]["batch_size"], seen[0]["lane_concurrency"]) == (5, {"mail": 1})


_TENANT: ContextVar[str | None] = ContextVar("_TENANT", default=None)
_LOCAL = threading.local()
_SEEN: list[tuple[str | None, str | None, bool]] = []
_THREADS: list[tuple[str, int]] = []


@dataclass(frozen=True, slots=True)
class Pinged:
    value: int


def tenant_startup(**kwargs: object) -> None:
    _TENANT.set("acme")
    _LOCAL.tenant = "acme"


def _ping(evt: Pinged) -> None:
    on_main = threading.current_thread() is threading.main_thread()
    _SEEN.append((_TENANT.get(), getattr(_LOCAL, "tenant", None), on_main))
    transaction.on_commit(lambda: _THREADS.append(("on_commit", threading.get_ident())))


@contextmanager
def thread_around(ctx: object) -> Iterator[None]:
    _THREADS.append(("enter", threading.get_ident()))
    yield
    _THREADS.append(("exit", threading.get_ident()))


@pytest.mark.usefixtures("skip_locked_backend")
def test_threaded_rows_see_relay_startups_context_vars_but_not_its_thread_locals(
    settings,
) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {
        "RELAY_STARTUP": f"{__name__}.tenant_startup",
        "AROUND_DELIVERY": f"{__name__}.thread_around",
    }
    _SEEN.clear()
    _THREADS.clear()
    entry = RegisteredReceiver(
        key="tests.ping",
        event_class=Pinged,
        func=_ping,
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
        lane="ping",
        give_up_after=timedelta(days=1),
    )
    out = StringIO()
    with event_registered(Pinged, "tests.Pinged"), receiver_registered(entry):
        with transaction.atomic():
            fire(Pinged(value=1))
            fire(Pinged(value=2))
        try:
            call_command(
                "deliver_events",
                "--lane",
                "ping",
                "--lane-concurrency",
                "ping=2",
                "--passes",
                "3",
                stdout=out,
            )
        finally:
            _TENANT.set(None)
            vars(_LOCAL).clear()

    assert _SEEN == [("acme", None, False), ("acme", None, False)]
    by_thread: dict[int, list[str]] = {}
    for step, ident in _THREADS:
        by_thread.setdefault(ident, []).append(step)
    assert threading.get_ident() not in by_thread
    triple = ["enter", "on_commit", "exit"]
    assert sorted(by_thread.values()) in ([triple, triple], [triple * 2])
    assert f"{DeliveryStatus.SUCCEEDED.value}: 2" in out.getvalue()
