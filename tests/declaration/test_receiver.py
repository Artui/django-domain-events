"""Tests mirroring ``django_domain_events/receiver.py``."""

from __future__ import annotations

from datetime import timedelta

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
    """The key is written onto delivery rows, so a consumer has to be able to
    pin it against a later rename."""
    assert registry.receiver_for_key("testapp.with_context").takes_context is True


def test_declared_modes_and_limits_are_recorded() -> None:
    by_key = {r.key: r for r in registry.receivers_for(OrderPlaced)}
    assert by_key["testapp.durable_receiver"].mode is DeliveryMode.DURABLE
    assert by_key["testapp.inline_receiver"].mode is DeliveryMode.INLINE
    assert by_key["testapp.on_commit_receiver"].mode is DeliveryMode.ON_COMMIT
    assert by_key["testapp.durable_receiver"].max_attempts == 5


def test_the_decorator_returns_the_function_unchanged() -> None:
    """Receivers stay ordinary callables so they remain unit-testable without
    going anywhere near the outbox."""
    from django_domain_events.declaration.receiver import receiver

    def plain(evt: OrderPlaced) -> None: ...

    assert receiver(OrderPlaced, key="testapp.plain")(plain) is plain
    registry._receivers.pop("testapp.plain", None)


def test_a_callable_with_no_name_refuses_to_guess_a_key() -> None:
    """A partial or a callable instance has no stable identity to derive from,
    and inventing one would write a key onto delivery rows that nothing can
    address later."""
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
    """Caught at declaration rather than when the relay reaches the row: a typo
    would otherwise mean the receiver quietly runs in the relay forever."""
    from django_domain_events.declaration.receiver import receiver

    with pytest.raises(ValueError, match="site must be"):
        receiver(OrderPlaced, site="celery")


def test_a_task_site_needs_a_durable_mode() -> None:
    """INLINE and ON_COMMIT have no delivery row, so there is nothing to hand a
    backend. Accepting the combination runs the receiver in the firing process
    while the declaration says otherwise."""
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
    """Same reasoning as site=. Accepting them would let a declaration state a
    retry budget for a receiver that has no row to carry it, and nothing
    downstream would say so: the catalogue would publish the number and the
    relay would ignore it."""
    with pytest.raises(ValueError, match=needle):

        @receiver(OrderPlaced, mode=DeliveryMode.INLINE, key="tests.bad_knob", **kwargs)
        def handler(evt: OrderPlaced) -> None: ...


@pytest.mark.parametrize("value", [0, -5])
def test_a_non_positive_lease_is_refused(value: int) -> None:
    """Zero expires the instant before the receiver starts, so a second relay
    reclaims the row immediately and both run it - the exact double delivery
    the lease exists to prevent."""
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
    """A zero base retries at once on every attempt, spending the whole budget
    in the time it takes to fail that many times; a zero cap does the same from
    the attempt it is reached."""
    with pytest.raises(ValueError, match=f"{name} must be positive"):
        receiver(OrderPlaced, key="tests.zero_curve", **{name: value})
    assert registry.receiver_for_key("tests.zero_curve") is None


def test_a_cap_below_the_base_is_refused() -> None:
    """Both declared, and contradicting each other: the cap would win on every
    attempt, so the declared base is a number nothing ever reads."""
    with pytest.raises(ValueError, match="backoff_cap_seconds=30 is below backoff_base_seconds=60"):
        receiver(OrderPlaced, key="tests.inverted", backoff_base_seconds=60, backoff_cap_seconds=30)


@pytest.mark.parametrize(
    "kwargs", [{"backoff_base_seconds": 7200}, {"backoff_cap_seconds": 1}], ids=["base", "cap"]
)
def test_either_half_of_the_curve_may_be_declared_alone(kwargs: dict[str, float]) -> None:
    """The other half is the setting, read when an attempt fails rather than
    here, so the comparison is made only when both are declared. Holds both
    ``is not None`` conjuncts of that check: without either, comparing a number
    with None raises."""

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
    """A blank lane cannot be named on the command line, so a receiver declared
    in one would be served by no relay at all."""
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
    """The same rule as the other row knobs: only a row is retried, and only a
    row is claimed by a relay serving a lane."""
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
    """Zero would dead-letter every deferral on arrival, which is a receiver
    that cannot be deferred at all, declared in a way that reads as patience."""
    with pytest.raises(ValueError, match="give_up_after must be a positive timedelta"):
        receiver(OrderPlaced, key="tests.no_patience", give_up_after=value)


@pytest.mark.parametrize("value", [3600, 3600.0, "1h"])
def test_a_give_up_after_that_is_not_a_timedelta_is_refused(value: object) -> None:
    """A bare number has no unit, and reading it as seconds or as days would
    each be the wrong guess for somebody."""
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
