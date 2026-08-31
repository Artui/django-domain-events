"""Tests mirroring ``django_domain_events/utils.py``."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from django_domain_events.utils import label_for, require_frozen_dataclass


def test_label_comes_from_the_app_label_not_the_import_path() -> None:
    """The path is a refactor away from orphaning every row that names it."""
    assert label_for("tests.testapp.events", "OrderPlaced") == "testapp.OrderPlaced"


def test_a_module_outside_any_installed_app_refuses() -> None:
    """A made-up label would be written onto rows and only surface later, as a
    mismatch nobody can trace back to this moment."""
    with pytest.raises(LookupError, match="not inside an installed app"):
        label_for("some.unrelated.module", "Thing")


def test_a_non_dataclass_is_rejected() -> None:
    class NotADataclass:
        pass

    with pytest.raises(TypeError, match="is not a dataclass"):
        require_frozen_dataclass(NotADataclass)


def test_a_mutable_dataclass_is_rejected() -> None:
    """At-least-once delivery hands a different instance to every attempt, so a
    receiver mutating one is writing to a copy it is about to discard."""

    @dataclass
    class Mutable:
        value: int

    with pytest.raises(TypeError, match="mutable dataclass"):
        require_frozen_dataclass(Mutable)


def test_a_frozen_dataclass_passes() -> None:
    @dataclass(frozen=True)
    class Frozen:
        value: int

    assert require_frozen_dataclass(Frozen) is None
