from __future__ import annotations

import dataclasses
import json

from django_domain_events.types.catalogue import Catalogue
from django_domain_events.types.catalogue_event import CatalogueEvent
from django_domain_events.types.catalogue_receiver import CatalogueReceiver
from django_domain_events.types.retention import Retention


def render_catalogue(catalogue: Catalogue, *, format: str = "markdown") -> str:
    """Render a catalogue as a document, in Markdown or JSON, ending in exactly one newline."""
    if format == "json":
        return json.dumps(dataclasses.asdict(catalogue), indent=2, sort_keys=False) + "\n"
    if format == "markdown":
        return _markdown(catalogue)
    raise ValueError(f"Unknown catalogue format {format!r}. Use 'markdown' or 'json'.")


def _markdown(catalogue: Catalogue) -> str:
    lines = ["# Event catalogue", ""]
    wildcards = bool(catalogue.wildcard_receivers)
    if wildcards:
        # The wildcard section comes first, because every section below refers to it.
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
        retention = _retention(event)
        if retention:
            lines += [retention, ""]
        lines += ["| Field | Type | Required | Default |", "| --- | --- | --- | --- |"]
        for field in event.fields:
            required = "yes" if field.required else "no"
            default = "-" if field.default is None else f"`{_cell(field.default)}`"
            lines.append(
                f"| `{_cell(field.name)}` | `{_cell(field.type)}` | {required} | {default} |"
            )
        lines.append("")
        if not event.receivers:
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
        # Blanked rather than printed for a mode they do not apply to.
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
    for receiver in receivers:
        if receiver.targets is not None:
            lines += [
                f"`{_cell(receiver.key)}` writes one delivery per target returned by "
                f"`{receiver.targets}`.",
                "",
            ]
        if receiver.backoff_base_seconds is not None or receiver.backoff_cap_seconds is not None:
            base = _seconds(receiver.backoff_base_seconds, "BACKOFF_BASE_SECONDS")
            cap = _seconds(receiver.backoff_cap_seconds, "BACKOFF_CAP_SECONDS")
            lines += [
                f"`{_cell(receiver.key)}` retries on its own curve: base {base}, cap {cap}.",
                "",
            ]
        if receiver.lane != "default":
            lines += [
                f"`{_cell(receiver.key)}` is served by relays started with "
                f"`--lane {_cell(receiver.lane)}`.",
                "",
            ]
        if receiver.give_up_after_seconds is not None:
            lines += [
                f"`{_cell(receiver.key)}` dead-letters a deferral once its delivery has "
                f"been owed for {_plain(receiver.give_up_after_seconds)}.",
                "",
            ]
        if receiver.due_seconds is not None:
            lines += [
                f"`{_cell(receiver.key)}` is due {_plain(receiver.due_seconds)} after the event "
                f"occurred.",
                "",
            ]
        if receiver.due_callable is not None:
            lines += [
                f"`{_cell(receiver.key)}` is due at the time `{receiver.due_callable}` returns "
                f"for the event.",
                "",
            ]
    return lines


_POLICIES = {
    Retention.SUCCEEDED.value: (
        "Deleted once every delivery has succeeded or been cancelled. A dead "
        "letter keeps it for `RETENTION_DAYS`, so it can still be requeued."
    ),
    Retention.SETTLED.value: (
        "Deleted once every delivery is terminal, dead letters included, rather "
        "than after `RETENTION_DAYS`."
    ),
}


def _retention(event: CatalogueEvent) -> str:
    """A sentence for an event kept other than for ``RETENTION_DAYS``, or blank."""
    if event.retention_seconds is not None:
        return f"Kept for {_duration(event.retention_seconds)} rather than `RETENTION_DAYS`."
    return _POLICIES.get(event.delete_when, "")


def _duration(seconds: int) -> str:
    """The largest whole unit a window is declared in, so a week reads as days."""
    unit, count = "second", seconds
    for name, size in (("day", 86400), ("hour", 3600)):
        if seconds % size == 0:
            unit, count = name, seconds // size
            break
    return f"{count} {unit}{'' if count == 1 else 's'}"


def _seconds(value: float | None, setting: str) -> str:
    """A declared duration as ``60s``, or the setting that supplies it."""
    return f"`{setting}`" if value is None else _plain(value)


def _plain(value: float) -> str:
    """A duration in seconds as ``1209600s``: fifteen significant digits, not six."""
    return f"{value:.15g}s"


def _cell(value: str) -> str:
    """Escape a pipe so it stays in its table cell; the split happens before inline parsing."""
    return value.replace("|", "\\|")


def _joined(lines: list[str]) -> str:
    """One trailing newline, never two: every section appends a blank separator."""
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines) + "\n"
