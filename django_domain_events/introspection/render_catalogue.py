from __future__ import annotations

import dataclasses
import json

from django_domain_events.types.catalogue import Catalogue
from django_domain_events.types.catalogue_receiver import CatalogueReceiver


def render_catalogue(catalogue: Catalogue, *, format: str = "markdown") -> str:
    """Render a catalogue as a document, in Markdown or JSON.

    Both, because they answer different questions: Markdown is read by a person
    onboarding onto a codebase, JSON is diffed by a pipeline that wants to fail
    a pull request for removing a field other teams consume.

    Both end in exactly one newline. These are written to a file and diffed,
    and a file with no final newline reports a change on its last line forever.
    """
    if format == "json":
        return json.dumps(dataclasses.asdict(catalogue), indent=2, sort_keys=False) + "\n"
    if format == "markdown":
        return _markdown(catalogue)
    raise ValueError(f"Unknown catalogue format {format!r}. Use 'markdown' or 'json'.")


def _markdown(catalogue: Catalogue) -> str:
    lines = ["# Event catalogue", ""]
    wildcards = bool(catalogue.wildcard_receivers)
    if wildcards:
        # First, because every section below refers to it, and a reader told
        # "plus every wildcard receiver" should not have to scroll past two
        # hundred events to find out who that is.
        lines += [
            "## Every event",
            "",
            "Declared for `AnyEvent`, so owed every event in this catalogue.",
            "",
        ]
        lines += _receiver_table(catalogue.wildcard_receivers)
    if not catalogue.events:
        lines.append("No events are declared.")
        return _joined(lines)
    for event in catalogue.events:
        lines += [f"## `{event.name}` (v{event.version})", "", f"`{event.class_path}`", ""]
        if event.doc:
            lines += [event.doc, ""]
        if event.migrates_older_rows:
            lines += [
                "Declares `upgrade()`, so rows written under an older version "
                "are migrated before they are decoded.",
                "",
            ]
        lines += ["| Field | Type | Required | Default |", "| --- | --- | --- | --- |"]
        for field in event.fields:
            required = "yes" if field.required else "no"
            default = "-" if field.default is None else f"`{_cell(field.default)}`"
            lines.append(
                f"| `{_cell(field.name)}` | `{_cell(field.type)}` | {required} | {default} |"
            )
        lines.append("")
        if not event.receivers:
            # Worth saying rather than leaving the section empty: an event with
            # no receivers is a real finding, and the usual reason to read a
            # catalogue at all. Said differently when a wildcard exists, because
            # then "nothing listens" is false.
            lines += [
                "No receiver is declared for this event alone; every wildcard receiver "
                "still receives it."
                if wildcards
                else "Nothing listens to this event.",
                "",
            ]
            continue
        lines += _receiver_table(event.receivers)
        if wildcards:
            lines += ["Plus every wildcard receiver.", ""]
    return _joined(lines)


def _receiver_table(receivers: tuple[CatalogueReceiver, ...]) -> list[str]:
    lines = [
        "| Receiver | Mode | Site | Max attempts | Eager | Lease |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for receiver in receivers:
        # Blanked rather than printed for a mode they do not apply to. The
        # declaration carries defaults nobody chose, and "5" beside an
        # INLINE receiver reads as a retry budget it will never have.
        durable = receiver.mode == "durable"
        site = _cell(receiver.site) if durable else "-"
        attempts = str(receiver.max_attempts) if durable else "-"
        eager = ("yes" if receiver.eager else "no") if durable else "-"
        lease = "default" if receiver.lease_seconds is None else f"{receiver.lease_seconds}s"
        lines.append(
            f"| `{_cell(receiver.key)}` | {_cell(receiver.mode)} | {site} | "
            f"{attempts} | {eager} | {lease if durable else '-'} |"
        )
    lines.append("")
    # Prose under the table rather than a seventh column. A column would
    # change every table in every catalogue a project has already committed,
    # for a property most receivers do not have.
    for receiver in receivers:
        if receiver.targets is not None:
            lines += [
                f"`{_cell(receiver.key)}` writes one delivery per target returned by "
                f"`{receiver.targets}`.",
                "",
            ]
    return lines


def _cell(value: str) -> str:
    """Escape a pipe so it stays inside its table cell.

    A GitHub-flavoured table is split on pipes *before* inline parsing, so
    backticks do not protect one: ``str | None`` - the commonest annotation
    there is - silently becomes two cells, shifting every later column and
    reporting an optional field as required. The escape survives inside a code
    span, and Python-Markdown tolerates it too, which is why a mkdocs preview
    cannot catch the unescaped version.
    """
    return value.replace("|", "\\|")


def _joined(lines: list[str]) -> str:
    """One trailing newline, never two.

    Every section appends a blank separator, so the last one leaves a blank
    line at the end of the file.
    """
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines) + "\n"
