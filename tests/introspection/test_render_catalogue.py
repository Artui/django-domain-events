"""Tests mirroring ``django_domain_events/render_catalogue.py``."""

from __future__ import annotations

import json
import re

import pytest

from django_domain_events.introspection.catalogue import catalogue
from django_domain_events.introspection.render_catalogue import render_catalogue
from django_domain_events.types.catalogue import Catalogue
from django_domain_events.types.catalogue_event import CatalogueEvent
from django_domain_events.types.catalogue_receiver import CatalogueReceiver


def test_markdown_names_every_event_and_its_receivers() -> None:
    document = render_catalogue(catalogue())
    assert "# Event catalogue" in document
    assert "## `testapp.OrderPlaced` (v1)" in document
    assert "`testapp.durable_receiver`" in document
    assert "| `tags` | `list[str]` | yes | - |" in document


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


def _cells(row: str) -> list[str]:
    """Split a row the way a GitHub-flavoured table parser does.

    On pipes that are not escaped, and before any inline parsing - which is
    exactly why a backtick does not protect one.
    """
    parts = re.split(r"(?<!\\)\|", row)
    return [p.strip() for p in parts[1:-1]]


def test_a_union_type_stays_inside_its_cell() -> None:
    """``str | None`` is the commonest annotation there is, and an unescaped
    pipe shifts every later column - reporting an optional field as required,
    in the artefact whose whole purpose is being read and diffed."""
    document = render_catalogue(catalogue())
    row = next(line for line in document.splitlines() if line.startswith("| `note`"))
    assert _cells(row) == ["`note`", "`str \\| None`", "no", "`None`"]


def test_every_table_row_has_the_width_of_its_header() -> None:
    """The failure this guards is silent: a shifted column still renders, it
    just says something false."""
    width = None
    for line in render_catalogue(catalogue()).splitlines():
        if not line.startswith("|"):
            width = None
            continue
        if width is None:
            width = len(_cells(line))
        assert len(_cells(line)) == width, line


def test_the_declared_lease_is_rendered() -> None:
    document = render_catalogue(catalogue())
    row = next(line for line in document.splitlines() if "testapp.slow_receiver" in line)
    assert _cells(row)[-1] == "1800s"


def test_knobs_that_do_not_apply_to_the_mode_are_blanked() -> None:
    """The declaration carries defaults nobody chose, and `5` beside an INLINE
    receiver reads as a retry budget it will never have."""
    document = render_catalogue(catalogue())
    row = next(line for line in document.splitlines() if "`testapp.inline_receiver`" in line)
    assert _cells(row) == ["`testapp.inline_receiver`", "inline", "-", "-", "-", "-"]


def test_a_durable_receiver_still_shows_its_knobs() -> None:
    document = render_catalogue(catalogue())
    row = next(line for line in document.splitlines() if "`testapp.durable_receiver`" in line)
    assert _cells(row) == ["`testapp.durable_receiver`", "durable", "relay", "5", "no", "default"]


def test_the_upgrade_hook_is_called_out_in_prose() -> None:
    from dataclasses import dataclass

    from tests.conftest import event_registered

    @dataclass(frozen=True)
    class Noted:
        value: int

        @staticmethod
        def upgrade(payload, from_version):
            return payload

    with event_registered(Noted, "tests.noted"):
        document = render_catalogue(catalogue())
    section = document.split("## `tests.noted`")[1].split("## ")[0]
    assert "Declares `upgrade()`" in section


def _receiver(key: str, *, targets: str | None = None) -> CatalogueReceiver:
    return CatalogueReceiver(
        key=key,
        callable_path=f"shop.receivers.{key}",
        mode="durable",
        site="relay",
        max_attempts=5,
        eager=False,
        takes_context=True,
        targets=targets,
    )


def _event(name: str, *receivers: CatalogueReceiver) -> CatalogueEvent:
    return CatalogueEvent(
        name=name,
        version=1,
        class_path=f"shop.events.{name}",
        doc="",
        fields=(),
        receivers=receivers,
    )


WITH_WILDCARD = Catalogue(
    events=(_event("shop.Heard", _receiver("shop.audit")), _event("shop.Unheard")),
    wildcard_receivers=(_receiver("hooks.deliver", targets="hooks.targets.owed"),),
)


def test_wildcards_are_listed_once_before_the_events() -> None:
    document = render_catalogue(WITH_WILDCARD)
    head, _, events = document.partition("## `shop.Heard`")
    assert "## Every event" in head
    assert "| `hooks.deliver` | durable | relay | 5 | no | default |" in head
    assert "hooks.deliver" not in events


def test_each_event_says_plus_every_wildcard_rather_than_listing_them() -> None:
    document = render_catalogue(WITH_WILDCARD)
    heard = document.split("## `shop.Heard`")[1].split("## ")[0]
    assert "`shop.audit`" in heard
    assert "Plus every wildcard receiver." in heard


def test_an_event_with_only_wildcards_does_not_claim_nothing_listens() -> None:
    """ "Nothing listens" would be false: every wildcard still receives it."""
    document = render_catalogue(WITH_WILDCARD)
    unheard = document.split("## `shop.Unheard`")[1]
    assert "Nothing listens to this event." not in unheard
    assert "every wildcard receiver still receives it" in unheard


def test_a_fan_out_says_where_its_targets_come_from() -> None:
    document = render_catalogue(WITH_WILDCARD)
    assert (
        "`hooks.deliver` writes one delivery per target returned by `hooks.targets.owed`."
        in document
    )


def test_wildcards_are_rendered_even_with_no_events_declared() -> None:
    document = render_catalogue(
        Catalogue(events=(), wildcard_receivers=(_receiver("hooks.deliver"),))
    )
    assert "## Every event" in document
    assert "No events are declared." in document


def test_without_wildcards_no_section_and_no_plus_line() -> None:
    document = render_catalogue(catalogue())
    assert "## Every event" not in document
    assert "Plus every wildcard receiver." not in document


def test_json_carries_the_wildcards_and_the_targets() -> None:
    parsed = json.loads(render_catalogue(WITH_WILDCARD, format="json"))
    assert [r["key"] for r in parsed["wildcard_receivers"]] == ["hooks.deliver"]
    assert parsed["wildcard_receivers"][0]["targets"] == "hooks.targets.owed"
    assert parsed["events"][0]["receivers"][0]["targets"] is None
