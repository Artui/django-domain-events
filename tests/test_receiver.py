"""Tests mirroring ``django_domain_events/receiver.py``."""

from __future__ import annotations

import pytest

from django_domain_events.receiver import receiver
from django_domain_events.registry import registry
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
    from django_domain_events.receiver import receiver

    def plain(evt: OrderPlaced) -> None: ...

    assert receiver(OrderPlaced, key="testapp.plain")(plain) is plain
    registry._receivers.pop("testapp.plain", None)


def test_a_callable_with_no_name_refuses_to_guess_a_key() -> None:
    """A partial or a callable instance has no stable identity to derive from,
    and inventing one would write a key onto delivery rows that nothing can
    address later."""
    from functools import partial

    from django_domain_events.receiver import receiver

    def target(evt: OrderPlaced, extra: int) -> None: ...

    with pytest.raises(TypeError, match="no __name__"):
        receiver(OrderPlaced)(partial(target, extra=1))


def test_such_a_callable_is_fine_with_an_explicit_key() -> None:
    from functools import partial

    from django_domain_events.receiver import receiver

    def target(evt: OrderPlaced, extra: int) -> None: ...

    bound = partial(target, extra=1)
    assert receiver(OrderPlaced, key="testapp.partial")(bound) is bound
    registry._receivers.pop("testapp.partial", None)


def test_an_unknown_execution_site_is_refused() -> None:
    """Caught at declaration rather than when the relay reaches the row: a typo
    would otherwise mean the receiver quietly runs in the relay forever."""
    from django_domain_events.receiver import receiver

    with pytest.raises(ValueError, match="site must be"):
        receiver(OrderPlaced, site="celery")


def test_a_task_site_needs_a_durable_mode() -> None:
    """INLINE and ON_COMMIT have no delivery row, so there is nothing to hand a
    backend. Accepting the combination runs the receiver in the firing process
    while the declaration says otherwise."""
    from django_domain_events.receiver import receiver
    from django_domain_events.types.delivery_mode import DeliveryMode

    with pytest.raises(ValueError, match="needs mode=DURABLE"):
        receiver(OrderPlaced, mode=DeliveryMode.INLINE, site="task")


@pytest.mark.parametrize(
    ("kwargs", "needle"),
    [
        ({"max_attempts": 8}, "max_attempts=8 needs mode=DURABLE"),
        ({"eager": True}, "eager=True needs mode=DURABLE"),
        ({"lease_seconds": 60}, "lease_seconds=60 needs mode=DURABLE"),
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
