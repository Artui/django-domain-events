"""An event name longer than the column that stores it is refused where it is declared.

``EventRecord.name`` is ``CharField(max_length=255)``. SQLite does not enforce
the length, so a longer name works there; Postgres raises ``DataError`` inside
``fire()``, in the caller's transaction. The mistake is made at declaration, so
that is where it is refused.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import make_dataclass

import pytest

import django_domain_events.declaration.registry as registry_module
from django_domain_events.declaration.event import event
from django_domain_events.declaration.registry import registry
from django_domain_events.models.event_record import EventRecord
from django_domain_events.utils import label_for

# Read from the model rather than from the constant under test, so these tests
# cannot die at import on a tree without the constant, and so the constant is
# pinned to the column it protects without being imported.
LIMIT = EventRecord._meta.get_field("name").max_length


@contextmanager
def _declared(name: str) -> Iterator[None]:
    """Always take an event registered under ``name`` out again."""
    try:
        yield
    finally:
        entry = registry._events_by_name.pop(name, None)
        if entry is not None:
            registry._events_by_class.pop(entry.event_class, None)


def _plain_class(label: str = "Plain") -> type:
    return make_dataclass(label, [], frozen=True)


def _derived_class(length: int) -> type:
    """A frozen dataclass whose derived name is exactly ``length`` characters long.

    The prefix is asked of ``label_for`` rather than written down, so the test
    follows the app label of whichever app the module belongs to.
    """
    # tests.testapp.events is inside the installed ``testapp``, which this test
    # module is not.
    module = "tests.testapp.events"
    prefix = label_for(module, "")
    cls = _plain_class("x" * (length - len(prefix)))
    cls.__module__ = module
    return cls


def _snapshot() -> tuple[dict[str, object], dict[type, object]]:
    return dict(registry._events_by_name), dict(registry._events_by_class)


def test_the_limit_is_the_width_of_the_column_that_stores_the_name() -> None:
    """Pins the constant the decorator checks against to the column it protects.

    The decorator cannot read the field: it runs while apps are still loading,
    and importing a model there raises ``AppRegistryNotReady``. So it carries
    the number, and this holds the number to the column.
    """
    assert LIMIT == 255
    # Read off the module rather than imported by name, so a tree without the
    # constant fails this one test instead of failing the whole file at collection.
    assert registry_module.MAX_NAME_LENGTH == LIMIT


def test_a_name_of_exactly_the_limit_is_accepted() -> None:
    """Holds the comparison: ``>=`` instead of ``>`` refuses this name.

    Passes on a tree without the check, as it must; it is here for the mutant.
    """
    name = "n" * LIMIT
    with _declared(name):
        cls = event(name=name)(_plain_class())
        entry = registry.event_for_name(name)
        assert entry is not None
        assert entry.event_class is cls


def test_a_name_one_over_the_limit_is_refused_naming_length_and_limit() -> None:
    name = "n" * (LIMIT + 1)
    with _declared(name), pytest.raises(ValueError) as caught:
        event(name=name)(_plain_class())
    message = str(caught.value)
    assert f"{LIMIT + 1} characters" in message
    assert f"the limit is {LIMIT}" in message
    assert "name=" in message
    # Both refusals are ValueError; this must be ours and not the registry's.
    assert "already registered" not in message


def test_a_refused_name_registers_nothing() -> None:
    before = _snapshot()
    name = "n" * (LIMIT + 1)
    cls = _plain_class()
    with _declared(name):
        with pytest.raises(ValueError, match="characters"):
            event(name=name)(cls)
        assert registry.event_for_name(name) is None
        assert registry.event_for_class(cls) is None
    assert _snapshot() == before


def test_a_derived_name_of_exactly_the_limit_is_accepted() -> None:
    cls = _derived_class(LIMIT)
    name = label_for(cls.__module__, cls.__name__)
    assert len(name) == LIMIT
    with _declared(name):
        assert event(cls) is cls
        assert registry.event_for_name(name) is not None


def test_a_derived_name_over_the_limit_is_refused() -> None:
    """Holds the derived-name path: a check applied only to an explicit ``name=``
    lets this through. Sized at one over, so the boundary is what answers."""
    cls = _derived_class(LIMIT + 1)
    name = label_for(cls.__module__, cls.__name__)
    assert len(name) == LIMIT + 1
    before = _snapshot()
    with _declared(name):
        with pytest.raises(ValueError, match=f"{LIMIT + 1} characters") as caught:
            event(cls)
        assert registry.event_for_name(name) is None
    assert "already registered" not in str(caught.value)
    assert _snapshot() == before


def test_a_long_derived_name_is_refused_through_the_called_form_too() -> None:
    """The called ``@event(...)`` and the bare ``@event`` share one ``decorate``;
    this holds the called one, with no ``name=`` so the name is derived."""
    cls = _derived_class(LIMIT + 1)
    name = label_for(cls.__module__, cls.__name__)
    with _declared(name), pytest.raises(ValueError, match=f"{LIMIT + 1} characters"):
        event(version=1)(cls)
    assert registry.event_for_name(name) is None
