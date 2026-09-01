"""Tests mirroring ``django_domain_events/what_listens_to.py``."""

from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.what_listens_to import what_listens_to
from tests.conftest import receiver_deleted
from tests.testapp.events import Eagerly, OrderPlaced, Unheard


def test_it_returns_every_receiver_sorted_by_key() -> None:
    keys = [r.key for r in what_listens_to(OrderPlaced)]
    assert keys == sorted(keys)
    assert "testapp.durable_receiver" in keys
    assert "testapp.with_context" in keys


def test_it_spans_every_mode() -> None:
    """Not only the durable ones: "who reacts to this" is a question about the
    code, and an inline receiver is as much a reaction as a queued one."""
    modes = {r.mode.value for r in what_listens_to(OrderPlaced)}
    assert modes == {"durable", "inline", "on_commit"}


def test_an_event_nobody_listens_to_returns_nothing() -> None:
    assert what_listens_to(Unheard) == []


def test_an_unregistered_class_returns_nothing_rather_than_raising() -> None:
    @dataclass(frozen=True)
    class Stranger:
        value: int

    assert what_listens_to(Stranger) == []


def test_deleting_a_receiver_removes_it_from_the_answer() -> None:
    with receiver_deleted("testapp.eager"):
        assert [r.key for r in what_listens_to(Eagerly)] == ["testapp.not_eager"]
