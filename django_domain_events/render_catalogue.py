from __future__ import annotations

import dataclasses
import json

from django_domain_events.types.catalogue import Catalogue


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
    if not catalogue.events:
        lines.append("No events are declared.")
        return _joined(lines)
    for event in catalogue.events:
        lines += [f"## `{event.name}` (v{event.version})", "", f"`{event.class_path}`", ""]
        if event.doc:
            lines += [event.doc, ""]
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
            # catalogue at all.
            lines += ["Nothing listens to this event.", ""]
            continue
        lines += [
            "| Receiver | Mode | Site | Max attempts | Eager |",
            "| --- | --- | --- | --- | --- |",
        ]
        for receiver in event.receivers:
            lines.append(
                f"| `{_cell(receiver.key)}` | {_cell(receiver.mode)} | "
                f"{_cell(receiver.site)} | {receiver.max_attempts} | "
                f"{'yes' if receiver.eager else 'no'} |"
            )
        lines.append("")
    return _joined(lines)


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
