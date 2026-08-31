"""Tests mirroring ``django_domain_events/render_catalogue.py``."""

from __future__ import annotations

import json

import pytest

from django_domain_events.catalogue import catalogue
from django_domain_events.render_catalogue import render_catalogue
from django_domain_events.types.catalogue import Catalogue


def test_markdown_names_every_event_and_its_receivers() -> None:
    document = render_catalogue(catalogue())
    assert "# Event catalogue" in document
    assert "## `testapp.OrderPlaced` (v1)" in document
    assert "`testapp.durable_receiver`" in document
    assert "| `tags` | `list[str]` | yes | - |" in document
    assert "| `note` | `str | None` | no | `None` |" in document


def test_markdown_says_when_nothing_listens() -> None:
    """An event with no receivers is a finding, and the usual reason to read a
    catalogue at all. An empty section would read as a rendering bug."""
    document = render_catalogue(catalogue())
    section = document.split("## `testapp.Unheard`")[1].split("## ")[0]
    assert "Nothing listens to this event." in section


def test_markdown_carries_the_docstring_when_there_is_one() -> None:
    document = render_catalogue(catalogue())
    assert "Every scalar the default codec claims" in document


def test_an_empty_catalogue_says_so() -> None:
    assert "No events are declared." in render_catalogue(Catalogue(events=()))


def test_json_is_parseable_and_keeps_the_structure() -> None:
    parsed = json.loads(render_catalogue(catalogue(), format="json"))
    events = {e["name"]: e for e in parsed["events"]}
    assert events["testapp.pinned"]["version"] == 3
    order = events["testapp.OrderPlaced"]
    assert {f["name"] for f in order["fields"]} >= {"order_id", "note"}
    assert any(r["key"] == "testapp.with_context" for r in order["receivers"])


def test_json_is_stable_across_calls() -> None:
    """It is written to a file and diffed. Import order is not a difference."""
    assert render_catalogue(catalogue(), format="json") == render_catalogue(
        catalogue(), format="json"
    )


def test_an_unknown_format_is_refused_by_name() -> None:
    with pytest.raises(ValueError, match="Unknown catalogue format 'yaml'"):
        render_catalogue(catalogue(), format="yaml")


def test_every_document_ends_with_exactly_one_newline() -> None:
    """It is written to a file, and a file that does not end in a newline is a
    diff that reports a change on the last line forever."""
    for form in ("markdown", "json"):
        document = render_catalogue(catalogue(), format=form)
        assert document.endswith("\n")
        assert not document.endswith("\n\n")
