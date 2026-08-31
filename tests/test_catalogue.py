"""Tests mirroring ``django_domain_events/catalogue.py``."""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from typing import Literal

from django_domain_events.catalogue import catalogue
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.conftest import event_registered, receiver_registered


def _by_name(name: str):
    return next(e for e in catalogue().events if e.name == name)


def test_every_declared_event_appears_sorted_by_name() -> None:
    names = [e.name for e in catalogue().events]
    assert names == sorted(names)
    assert {"testapp.OrderPlaced", "testapp.Unheard", "testapp.pinned"} <= set(names)


def test_the_pinned_name_and_version_are_the_registered_ones() -> None:
    """The catalogue publishes the identity rows are written under, not the
    class name: those differ the moment anyone passes name=."""
    entry = _by_name("testapp.pinned")
    assert entry.version == 3
    assert entry.class_path == "tests.testapp.events.PinnedName"


def test_fields_carry_type_requiredness_and_default() -> None:
    fields = {f.name: f for f in _by_name("testapp.OrderPlaced").fields}
    assert fields["order_id"].required is True
    assert fields["order_id"].default is None
    assert fields["note"].required is False
    assert fields["note"].default == "None"


def test_a_parameterised_generic_keeps_its_parameters() -> None:
    """``list[str].__name__`` is ``list``, so a catalogue built on __name__
    says a field is a list without saying of what."""
    fields = {f.name: f for f in _by_name("testapp.OrderPlaced").fields}
    assert fields["tags"].type == "list[str]"
    assert fields["kind"].type == "typing.Literal['retail', 'wholesale']"
    assert fields["total"].type == "Decimal"


def test_receivers_are_listed_with_their_declaration() -> None:
    receivers = {r.key: r for r in _by_name("testapp.OrderPlaced").receivers}
    assert [r.key for r in _by_name("testapp.OrderPlaced").receivers] == sorted(receivers)
    durable = receivers["testapp.durable_receiver"]
    assert durable.mode == "durable"
    assert durable.site == "relay"
    assert durable.max_attempts == 5
    assert durable.eager is False
    assert receivers["testapp.with_context"].takes_context is True
    assert durable.callable_path == "tests.testapp.events.durable_receiver"


def test_an_event_nothing_listens_to_is_still_catalogued() -> None:
    """The finding a catalogue exists to surface."""
    assert _by_name("testapp.Unheard").receivers == ()


def test_a_written_docstring_is_published() -> None:
    assert _by_name("testapp.OrderPlaced").doc.startswith("Every scalar")


def test_the_docstring_dataclass_writes_for_you_is_not() -> None:
    """``@dataclass`` fills __doc__ with the signature when there is none, so
    reading it naively puts ``PinnedName(value: int)`` where a description goes.
    """
    assert _by_name("testapp.pinned").doc == ""


@dataclass(frozen=True)
class WithFactory:
    tags: list[str] = field(default_factory=list)


def test_a_default_factory_is_named_not_called() -> None:
    """Named ``list`` rather than ``factory``: the fallback string in _default
    is the literal ``factory``, so a factory called ``factory`` produces the
    same answer whether the code reads __name__ or falls through it."""
    with event_registered(WithFactory, "tests.with_factory"):
        entry = _by_name("tests.with_factory")
    assert entry.fields[0].required is False
    assert entry.fields[0].default == "list()"


def test_building_a_catalogue_never_runs_consumer_code() -> None:
    """A default_factory is arbitrary code from the consumer's codebase, and
    generating a document is not a reason to execute it."""
    called = []

    def make() -> list[str]:
        called.append(1)
        return []

    @dataclass(frozen=True)
    class Built:
        tags: list[str] = field(default_factory=make)

    with event_registered(Built, "tests.built"):
        assert _by_name("tests.built").fields[0].default == "make()"
    assert called == []


@dataclass(frozen=True)
class Documented:
    """A description that belongs to the base."""

    value: int


@dataclass(frozen=True)
class Undocumented(Documented):
    pass


def test_a_subclass_of_a_documented_event_publishes_no_description() -> None:
    """A class always carries its own __doc__ and never inherits one, so
    ``@dataclass`` writes the signature here too - and the catalogue must blank
    that rather than publish ``Undocumented(value: int)`` as a description."""
    with event_registered(Undocumented, "tests.undocumented"):
        assert _by_name("tests.undocumented").doc == ""


@dataclass(frozen=True)
class Unresolvable:
    thing: NeverImported  # noqa: F821


@dataclass(frozen=True)
class UnresolvableQuoted:
    thing: "NeverImported"  # noqa: F821, UP037


def test_an_unresolvable_annotation_falls_back_instead_of_raising() -> None:
    """A consumer with ``if TYPE_CHECKING:`` imports has already made
    ``get_type_hints`` fail. Failing on their behalf would take the whole
    catalogue down over one annotation."""
    with event_registered(Unresolvable, "tests.unresolvable"):
        entry = _by_name("tests.unresolvable")
    assert entry.fields[0].type == "NeverImported"


def test_quoting_a_forward_reference_does_not_change_the_catalogue() -> None:
    """The annotation arrives as source text, so the quotes are in the string.
    Two spellings of one type must not read as two types in a diff."""
    with event_registered(UnresolvableQuoted, "tests.unresolvable_quoted"):
        entry = _by_name("tests.unresolvable_quoted")
    assert entry.fields[0].type == "NeverImported"


@dataclass(frozen=True)
class Partialled:
    value: int


def _handler(prefix: str, evt: Partialled) -> None: ...


def test_a_receiver_with_no_qualname_still_gets_a_path() -> None:
    """A receiver is only required to be callable: a partial has no __name__."""
    entry = RegisteredReceiver(
        key="tests.partial",
        event_class=Partialled,
        func=functools.partial(_handler, "x"),
        mode=DeliveryMode.DURABLE,
        takes_context=False,
        max_attempts=5,
        eager=False,
        site="relay",
    )
    with event_registered(Partialled, "tests.partialled"), receiver_registered(entry):
        described = _by_name("tests.partialled").receivers[0]
    assert described.callable_path == "functools.partial"


def test_literal_annotations_survive_the_registry_round_trip() -> None:
    @dataclass(frozen=True)
    class Choice:
        pick: Literal["a", "b"]

    with event_registered(Choice, "tests.choice"):
        assert _by_name("tests.choice").fields[0].type == "typing.Literal['a', 'b']"
