"""Tests mirroring ``django_domain_events/render_catalogue.py``."""

from __future__ import annotations

import dataclasses
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


def _kept(retention_seconds: int | None = None, delete_when: str = "") -> CatalogueEvent:
    return CatalogueEvent(
        name="tests.kept",
        version=1,
        class_path="tests.Kept",
        doc="",
        fields=(),
        receivers=(),
        retention_seconds=retention_seconds,
        delete_when=delete_when,
    )


@pytest.mark.parametrize(
    ("event", "prose"),
    [
        (_kept(retention_seconds=7 * 86400), "Kept for 7 days rather"),
        (_kept(retention_seconds=86400), "Kept for 1 day rather"),
        (_kept(retention_seconds=2 * 3600), "Kept for 2 hours rather"),
        (_kept(retention_seconds=90), "Kept for 90 seconds rather"),
        (_kept(delete_when="succeeded"), "Deleted once every delivery has succeeded"),
        (_kept(delete_when="settled"), "Deleted once every delivery is terminal"),
    ],
    ids=["days", "one-day", "hours", "seconds", "succeeded", "settled"],
)
def test_a_retention_of_its_own_is_called_out_in_prose(event: CatalogueEvent, prose: str) -> None:
    assert prose in render_catalogue(Catalogue(events=(event,)))


def test_the_ordinary_window_is_not_mentioned() -> None:
    """Every event has it unless it says otherwise, and a line saying so under
    each would bury the ones that do."""
    document = render_catalogue(Catalogue(events=(_kept(),)))
    assert "Kept for" not in document
    assert "Deleted once" not in document


def test_json_carries_the_retention() -> None:
    document = render_catalogue(Catalogue(events=(_kept(delete_when="settled"),)), format="json")
    [event] = json.loads(document)["events"]
    assert (event["retention_seconds"], event["delete_when"]) == (None, "settled")


def _with(**fields: object) -> str:
    """The Markdown of one event whose one receiver declares ``fields``."""
    receiver = dataclasses.replace(_receiver("shop.mail"), **fields)
    return render_catalogue(Catalogue(events=(_event("shop.Sent", receiver),)))


def test_a_named_lane_is_said_in_prose_under_the_table() -> None:
    """Prose rather than a column, for the reason the targets line gives: a
    column would change every committed catalogue for a property most receivers
    do not have."""
    assert "`shop.mail` is served by relays started with `--lane mail`." in _with(lane="mail")
    assert "--lane" not in _with()


def test_a_declared_curve_is_said_in_prose_under_the_table() -> None:
    both = _with(backoff_base_seconds=60.0, backoff_cap_seconds=1200.0)
    assert "`shop.mail` retries on its own curve: base 60s, cap 1200s." in both
    base_only = _with(backoff_base_seconds=0.5)
    assert "base 0.5s, cap `BACKOFF_CAP_SECONDS`." in base_only
    cap_only = _with(backoff_cap_seconds=300)
    assert "base `BACKOFF_BASE_SECONDS`, cap 300s." in cap_only
    assert "own curve" not in _with()


def test_json_carries_the_curve_and_the_lane() -> None:
    receiver = dataclasses.replace(
        _receiver("shop.mail"), backoff_base_seconds=60.0, backoff_cap_seconds=1200.0, lane="mail"
    )
    parsed = json.loads(
        render_catalogue(Catalogue(events=(_event("shop.Sent", receiver),)), format="json")
    )
    [published] = parsed["events"][0]["receivers"]
    assert (
        published["backoff_base_seconds"],
        published["backoff_cap_seconds"],
        published["lane"],
    ) == (60.0, 1200.0, "mail")


def test_give_up_after_is_said_in_prose_under_the_table() -> None:
    assert "`shop.mail` dead-letters a deferral once its delivery has been owed for 21600s." in (
        _with(give_up_after_seconds=21600.0)
    )
    assert "dead-letters a deferral" not in _with()


def test_json_carries_give_up_after_in_seconds() -> None:
    """Seconds, as the curve is published, so the JSON stays plain numbers."""
    receiver = dataclasses.replace(_receiver("shop.mail"), give_up_after_seconds=21600.0)
    parsed = json.loads(
        render_catalogue(Catalogue(events=(_event("shop.Sent", receiver),)), format="json")
    )
    [published] = parsed["events"][0]["receivers"]
    assert published["give_up_after_seconds"] == 21600.0
