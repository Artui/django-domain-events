"""A receiver key longer than the column that stores it is refused where it is declared.

``DeliveryRecord.receiver_key`` and ``ReceiverLastSuccess.receiver_key`` are
``CharField(max_length=255)``. SQLite does not enforce the length, so a longer
key works there; Postgres either truncates it in the bulk insert, orphaning the
delivery as "renamed, moved or deleted", or raises ``DataError`` at fire time.
Either way the mistake is made at declaration, so that is where it is refused.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pytest

from django_domain_events.declaration.receiver import receiver
from django_domain_events.declaration.registry import registry
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.utils import label_for
from tests.testapp.events import OrderPlaced

# Read from the models rather than from the constant under test, so these tests
# cannot die at import on a tree without the constant; ``test_registry`` holds
# the constant to the columns.
LIMIT = DeliveryRecord._meta.get_field("receiver_key").max_length


def _plain(evt: OrderPlaced) -> None: ...


@contextmanager
def _declared(key: str) -> Iterator[None]:
    """Declare a receiver under ``key`` and always take it out again."""
    try:
        yield
    finally:
        registry._receivers.pop(key, None)


def _derived_function(length: int) -> Callable[[OrderPlaced], None]:
    """A function whose derived key is exactly ``length`` characters long.

    The prefix is asked of ``label_for`` rather than written down, so the test
    follows the app label of whichever app the module belongs to.
    """

    def fn(evt: OrderPlaced) -> None: ...

    # tests.testapp.events is inside the installed ``testapp``, which this test
    # module is not.
    fn.__module__ = "tests.testapp.events"
    prefix = label_for(fn.__module__, "")
    fn.__name__ = "x" * (length - len(prefix))
    return fn


def test_a_key_of_exactly_the_limit_is_accepted() -> None:
    """Holds the comparison: ``>=`` instead of ``>`` refuses this key.

    Passes on a tree without the check, as it must; it is here for the mutant.
    """
    key = "k" * LIMIT
    with _declared(key):
        receiver(OrderPlaced, key=key)(_plain)
        assert registry.receiver_for_key(key) is not None


def test_a_key_one_over_the_limit_is_refused_naming_length_and_limit() -> None:
    key = "k" * (LIMIT + 1)
    with _declared(key), pytest.raises(ValueError) as caught:
        receiver(OrderPlaced, key=key)(_plain)
    message = str(caught.value)
    assert f"{LIMIT + 1} characters" in message
    assert f"{LIMIT}" in message
    assert "key=" in message
    # Both refusals are ValueError; this must be ours and not the registry's.
    assert "already registered" not in message


def test_a_refused_key_registers_nothing() -> None:
    before = registry.receivers()
    key = "k" * (LIMIT + 1)
    with _declared(key):
        with pytest.raises(ValueError, match="characters"):
            receiver(OrderPlaced, key=key)(_plain)
        assert registry.receiver_for_key(key) is None
    assert registry.receivers() == before


def test_a_derived_key_of_exactly_the_limit_is_accepted() -> None:
    fn = _derived_function(LIMIT)
    key = label_for(fn.__module__, fn.__name__)
    assert len(key) == LIMIT
    with _declared(key):
        receiver(OrderPlaced)(fn)
        assert registry.receiver_for_key(key) is not None


def test_a_derived_key_over_the_limit_is_refused() -> None:
    """Holds the derived-key path: a check applied only to an explicit ``key=``
    lets this through. Sized at one over, so the boundary is what answers."""
    fn = _derived_function(LIMIT + 1)
    key = label_for(fn.__module__, fn.__name__)
    assert len(key) == LIMIT + 1
    before = registry.receivers()
    with _declared(key):
        with pytest.raises(ValueError, match=f"{LIMIT + 1} characters") as caught:
            receiver(OrderPlaced)(fn)
        assert registry.receiver_for_key(key) is None
    assert "already registered" not in str(caught.value)
    assert registry.receivers() == before


@pytest.mark.parametrize("mode", [DeliveryMode.INLINE, DeliveryMode.ON_COMMIT])
def test_a_long_key_is_refused_in_every_mode(mode: DeliveryMode) -> None:
    """Holds the rule as mode-blind: a check scoped to ``DURABLE`` lets these through.

    Neither mode writes the key to a column, so this is the rule rather than a
    database limit: a receiver's key stays valid when its mode changes.
    """
    key = "k" * (LIMIT + 1)
    with _declared(key), pytest.raises(ValueError, match=f"{LIMIT + 1} characters"):
        receiver(OrderPlaced, key=key, mode=mode)(_plain)
    assert registry.receiver_for_key(key) is None
