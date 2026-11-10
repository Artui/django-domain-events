from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from django_domain_events.declaration.any_event import AnyEvent
from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from tests.testapp.events import OrderPlaced


def test_the_default_key_comes_from_the_app_label() -> None:
    assert registry.receiver_for_key("testapp.durable_receiver") is not None


def test_an_explicit_key_is_used_verbatim() -> None:
    """The key is written onto delivery rows, so renames must not change it."""
    assert registry.receiver_for_key("testapp.with_context").takes_context is True


def test_declared_modes_and_limits_are_recorded() -> None:
    by_key = {r.key: r for r in registry.receivers_for(OrderPlaced)}
    assert by_key["testapp.durable_receiver"].mode is DeliveryMode.DURABLE
    assert by_key["testapp.inline_receiver"].mode is DeliveryMode.INLINE
    assert by_key["testapp.on_commit_receiver"].mode is DeliveryMode.ON_COMMIT
    assert by_key["testapp.durable_receiver"].max_attempts == 5


def test_the_decorator_returns_the_function_unchanged() -> None:
    """Receivers stay ordinary callables, unit-testable without the outbox."""
    from django_domain_events.declaration.receiver import receiver

    def plain(evt: OrderPlaced) -> None: ...

    assert receiver(OrderPlaced, key="testapp.plain")(plain) is plain
    registry._receivers.pop("testapp.plain", None)


def test_a_callable_with_no_name_refuses_to_guess_a_key() -> None:
    """A partial or callable instance has no stable identity to derive a key from."""
    from functools import partial

    from django_domain_events.declaration.receiver import receiver

    def target(evt: OrderPlaced, extra: int) -> None: ...

    with pytest.raises(TypeError, match="no __name__"):
        receiver(OrderPlaced)(partial(target, extra=1))


def test_such_a_callable_is_fine_with_an_explicit_key() -> None:
    from functools import partial

    from django_domain_events.declaration.receiver import receiver

    def target(evt: OrderPlaced, extra: int) -> None: ...

    bound = partial(target, extra=1)
    assert receiver(OrderPlaced, key="testapp.partial")(bound) is bound
    registry._receivers.pop("testapp.partial", None)


def test_an_unknown_execution_site_is_refused() -> None:
    """Caught at declaration: a typo would make the receiver run in the relay forever."""
    from django_domain_events.declaration.receiver import receiver

    with pytest.raises(ValueError, match="site must be"):
        receiver(OrderPlaced, site="celery")


def test_a_task_site_needs_a_durable_mode() -> None:
    """INLINE and ON_COMMIT have no delivery row, so there is nothing to hand a backend."""
    from django_domain_events.declaration.receiver import receiver
    from django_domain_events.types.delivery_mode import DeliveryMode

    with pytest.raises(ValueError, match="needs mode=DURABLE"):
        receiver(OrderPlaced, mode=DeliveryMode.INLINE, site="task")


@pytest.mark.parametrize(
    ("kwargs", "needle"),
    [
        ({"max_attempts": 8}, "max_attempts=8 needs mode=DURABLE"),
        ({"eager": True}, "eager=True needs mode=DURABLE"),
        ({"lease_seconds": 60}, "lease_seconds=60 needs mode=DURABLE"),
        ({"targets": lambda evt, ctx: []}, "targets=<function .*> needs mode=DURABLE"),
    ],
)
def test_row_shaped_knobs_are_refused_without_a_row(kwargs, needle) -> None:
    """Same reasoning as site=."""
    with pytest.raises(ValueError, match=needle):

        @receiver(OrderPlaced, mode=DeliveryMode.INLINE, key="tests.bad_knob", **kwargs)
        def handler(evt: OrderPlaced) -> None: ...


@pytest.mark.parametrize("value", [0, -5])
def test_a_non_positive_lease_is_refused(value: int) -> None:
    """Zero expires the lease at once, so two relays both run the row."""
    with pytest.raises(ValueError, match="lease_seconds must be positive"):

        @receiver(OrderPlaced, key="tests.zero_lease", lease_seconds=value)
        def handler(evt: OrderPlaced) -> None: ...


def test_a_targets_callable_is_recorded_on_the_registration() -> None:
    def owed(evt: OrderPlaced, ctx: DeliveryContext) -> list[str]:
        return []

    @receiver(OrderPlaced, key="tests.fan_out", targets=owed)
    def handler(evt: OrderPlaced) -> None: ...

    try:
        assert registry.receiver_for_key("tests.fan_out").targets is owed
        assert registry.receiver_for_key("testapp.durable_receiver").targets is None
    finally:
        registry._receivers.pop("tests.fan_out", None)


def test_a_targets_that_cannot_be_called_is_refused_at_the_decorator() -> None:
    """Otherwise the first fire() would raise inside somebody's transaction."""
    with pytest.raises(TypeError, match="targets must be callable, got list"):
        receiver(OrderPlaced, key="tests.not_callable", targets=["a", "b"])
    assert registry.receiver_for_key("tests.not_callable") is None


def test_a_wildcard_can_be_declared() -> None:
    @receiver(AnyEvent, key="tests.everything")
    def handler(evt: object) -> None: ...

    try:
        assert registry.receiver_for_key("tests.everything").event_class is AnyEvent
    finally:
        registry._receivers.pop("tests.everything", None)


def test_a_backoff_curve_is_recorded_on_the_registration() -> None:
    @receiver(OrderPlaced, key="tests.curve", backoff_base_seconds=60, backoff_cap_seconds=1200)
    def handler(evt: OrderPlaced) -> None: ...

    try:
        entry = registry.receiver_for_key("tests.curve")
        assert (entry.backoff_base_seconds, entry.backoff_cap_seconds) == (60, 1200)
        default = registry.receiver_for_key("testapp.durable_receiver")
        assert (default.backoff_base_seconds, default.backoff_cap_seconds) == (None, None)
    finally:
        registry._receivers.pop("tests.curve", None)


@pytest.mark.parametrize("name", ["backoff_base_seconds", "backoff_cap_seconds"])
@pytest.mark.parametrize("value", [0, -1.5])
def test_a_non_positive_backoff_is_refused(name: str, value: float) -> None:
    """A zero base retries at once and spends the budget; a zero cap does the same."""
    with pytest.raises(ValueError, match=f"{name} must be positive"):
        receiver(OrderPlaced, key="tests.zero_curve", **{name: value})
    assert registry.receiver_for_key("tests.zero_curve") is None


def test_a_cap_below_the_base_is_refused() -> None:
    """Both declared and contradicting: the cap wins, so the base is never read."""
    with pytest.raises(ValueError, match="backoff_cap_seconds=30 is below backoff_base_seconds=60"):
        receiver(OrderPlaced, key="tests.inverted", backoff_base_seconds=60, backoff_cap_seconds=30)


@pytest.mark.parametrize(
    "kwargs", [{"backoff_base_seconds": 7200}, {"backoff_cap_seconds": 1}], ids=["base", "cap"]
)
def test_either_half_of_the_curve_may_be_declared_alone(kwargs: dict[str, float]) -> None:
    """The setting is read when an attempt fails, so the comparison needs both declared."""

    @receiver(OrderPlaced, key="tests.half_curve", **kwargs)
    def handler(evt: OrderPlaced) -> None: ...

    registry._receivers.pop("tests.half_curve", None)


def test_a_lane_is_recorded_and_defaults_to_the_default_lane() -> None:
    @receiver(OrderPlaced, key="tests.mail", lane="mail")
    def handler(evt: OrderPlaced) -> None: ...

    try:
        assert registry.receiver_for_key("tests.mail").lane == "mail"
        assert registry.receiver_for_key("testapp.durable_receiver").lane == "default"
    finally:
        registry._receivers.pop("tests.mail", None)


@pytest.mark.parametrize("value", ["", None, 3])
def test_a_lane_must_be_a_non_empty_string(value: object) -> None:
    """A blank lane cannot be named on the command line, so no relay would serve it."""
    with pytest.raises(ValueError, match="lane must be a non-empty string"):
        receiver(OrderPlaced, key="tests.blank_lane", lane=value)


@pytest.mark.parametrize(
    ("kwargs", "needle"),
    [
        ({"backoff_base_seconds": 60}, "backoff_base_seconds=60 needs mode=DURABLE"),
        ({"backoff_cap_seconds": 60}, "backoff_cap_seconds=60 needs mode=DURABLE"),
        ({"lane": "mail"}, "lane='mail' needs mode=DURABLE"),
    ],
)
def test_the_curve_and_the_lane_are_refused_without_a_row(kwargs, needle) -> None:
    """Same rule as the other row knobs: only rows are retried or claimed by a lane."""
    with pytest.raises(ValueError, match=needle):
        receiver(OrderPlaced, mode=DeliveryMode.ON_COMMIT, key="tests.bad_curve", **kwargs)


# --- give_up_after ------------------------------------------------------------


def test_give_up_after_is_recorded_and_defaults_to_none() -> None:
    @receiver(OrderPlaced, key="tests.bounded", give_up_after=timedelta(hours=6))
    def handler(evt: OrderPlaced) -> None: ...

    try:
        assert registry.receiver_for_key("tests.bounded").give_up_after == timedelta(hours=6)
        assert registry.receiver_for_key("testapp.durable_receiver").give_up_after is None
    finally:
        registry._receivers.pop("tests.bounded", None)


@pytest.mark.parametrize("value", [timedelta(0), timedelta(seconds=-1)])
def test_a_give_up_after_that_is_not_in_the_future_is_refused(value: timedelta) -> None:
    """Zero would dead-letter every deferral on arrival, a receiver that cannot defer."""
    with pytest.raises(ValueError, match="give_up_after must be a positive timedelta"):
        receiver(OrderPlaced, key="tests.no_patience", give_up_after=value)


@pytest.mark.parametrize("value", [3600, 3600.0, "1h"])
def test_a_give_up_after_that_is_not_a_timedelta_is_refused(value: object) -> None:
    """A bare number has no unit, so reading it as seconds or days is a guess."""
    with pytest.raises(ValueError, match="give_up_after must be a positive timedelta"):
        receiver(OrderPlaced, key="tests.no_unit", give_up_after=value)


def test_give_up_after_is_refused_without_a_row() -> None:
    with pytest.raises(ValueError, match="give_up_after=.* needs mode=DURABLE"):
        receiver(
            OrderPlaced,
            mode=DeliveryMode.ON_COMMIT,
            key="tests.bad_bound",
            give_up_after=timedelta(hours=1),
        )


def _when(evt: OrderPlaced) -> datetime:
    return evt.placed_at


@pytest.mark.parametrize("due", [timedelta(hours=48), _when])
def test_a_due_time_is_recorded_on_the_registration(due: object) -> None:
    """Both spellings are accepted, which holds each half of the type check."""

    @receiver(OrderPlaced, key="tests.scheduled", due=due)
    def handler(evt: OrderPlaced) -> None: ...

    try:
        assert registry.receiver_for_key("tests.scheduled").due is due
        assert registry.receiver_for_key("testapp.durable_receiver").due is None
    finally:
        registry._receivers.pop("tests.scheduled", None)


@pytest.mark.parametrize("due", [3600, "48h", datetime(2026, 10, 1, tzinfo=timezone.utc)])
def test_a_due_that_is_neither_a_delay_nor_a_callable_is_refused(due: object) -> None:
    """A bare number has no unit, and a fixed datetime fires once for every event."""
    with pytest.raises(TypeError, match="due must be a timedelta or a callable"):
        receiver(OrderPlaced, key="tests.bad_due", due=due)
    assert registry.receiver_for_key("tests.bad_due") is None


@pytest.mark.parametrize("due", [timedelta(0), timedelta(hours=-1)])
def test_a_delay_that_is_not_in_the_future_is_refused(due: timedelta) -> None:
    """A delay of zero or less is a receiver that is not scheduled."""
    with pytest.raises(ValueError, match="due must be a positive timedelta"):
        receiver(OrderPlaced, key="tests.no_delay", due=due)


def test_a_due_time_is_refused_without_a_row() -> None:
    """A scheduled reaction is a delivery row waiting for its time."""
    with pytest.raises(ValueError, match="due=.* needs mode=DURABLE"):
        receiver(OrderPlaced, mode=DeliveryMode.ON_COMMIT, key="tests.inline_due", due=timedelta(1))


def test_a_due_time_is_refused_beside_eager() -> None:
    """eager means attempt at commit, which a reaction due in days cannot honour."""
    with pytest.raises(ValueError, match="due= cannot be combined with eager=True"):
        receiver(OrderPlaced, key="tests.eager_due", eager=True, due=timedelta(hours=1))


def test_a_due_time_is_refused_beside_targets() -> None:
    """A fan-out resolved at fire time would keep that recipient set for weeks."""
    with pytest.raises(ValueError, match="due= cannot be combined with targets="):
        receiver(
            OrderPlaced, key="tests.fan_due", targets=lambda e, c: ["a"], due=timedelta(hours=1)
        )


def test_a_due_time_runs_through_a_task_as_through_the_relay() -> None:
    """site="task" is accepted: the relay hands the row over at its due time."""

    @receiver(OrderPlaced, key="tests.task_due", site="task", due=timedelta(hours=1))
    def handler(evt: OrderPlaced) -> None: ...

    try:
        assert registry.receiver_for_key("tests.task_due").site == "task"
    finally:
        registry._receivers.pop("tests.task_due", None)


# targets= and due= stay synchronous


async def _async_targets(evt: OrderPlaced, ctx: DeliveryContext) -> list[str]:
    return ["a"]


async def _async_when(evt: OrderPlaced) -> datetime:
    return evt.placed_at


class _AsyncTargets:
    async def __call__(self, evt: OrderPlaced, ctx: DeliveryContext) -> list[str]:
        return ["a"]


@pytest.mark.parametrize("targets", [_async_targets, _AsyncTargets()], ids=["function", "object"])
def test_an_async_targets_callable_is_refused(targets: object) -> None:
    """Resolved in the caller's transaction on every fire, where a coroutine fails."""
    with pytest.raises(TypeError, match="targets= must be a plain function"):
        receiver(OrderPlaced, key="tests.async_targets", targets=targets)
    assert registry.receiver_for_key("tests.async_targets") is None


def test_an_async_due_callable_is_refused() -> None:
    """Resolved in the caller's transaction, and a coroutine is not a datetime."""
    with pytest.raises(TypeError, match="due= must be a plain function"):
        receiver(OrderPlaced, key="tests.async_due", due=_async_when)
    assert registry.receiver_for_key("tests.async_due") is None


def test_an_async_receiver_is_accepted() -> None:
    """Only the fire-time callables are refused; the receiver itself may be async."""

    @receiver(OrderPlaced, key="tests.async_receiver")
    async def handler(evt: OrderPlaced) -> None: ...

    try:
        assert registry.receiver_for_key("tests.async_receiver").func is handler
    finally:
        registry._receivers.pop("tests.async_receiver", None)
