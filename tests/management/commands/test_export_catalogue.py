"""Tests mirroring ``management/commands/export_catalogue.py``."""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from django.core.management import CommandError, call_command

from django_domain_events.catalogue import catalogue
from django_domain_events.render_catalogue import render_catalogue


def _run(*args: str) -> str:
    out = StringIO()
    call_command("export_catalogue", *args, stdout=out)
    return out.getvalue()


def test_it_writes_markdown_to_stdout_by_default() -> None:
    document = _run()
    assert document.startswith("# Event catalogue")
    assert "## `testapp.OrderPlaced` (v1)" in document


def test_it_writes_json_when_asked() -> None:
    parsed = json.loads(_run("--format", "json"))
    assert any(e["name"] == "testapp.pinned" for e in parsed["events"])


def test_stdout_is_byte_for_byte_the_rendered_document() -> None:
    """The renderer already ends in a newline. A command that adds another
    makes ``export_catalogue > catalogue.md`` differ from ``--output``."""
    assert _run() == render_catalogue(catalogue())
    assert not _run().endswith("\n\n")


def test_output_writes_the_same_bytes_to_a_file(tmp_path: Path) -> None:
    destination = tmp_path / "catalogue.md"
    assert f"wrote {destination}" in _run("--output", str(destination))
    assert destination.read_text(encoding="utf-8") == _run()


def test_an_unknown_format_is_refused_by_the_parser() -> None:
    with pytest.raises(CommandError, match="invalid choice"):
        _run("--format", "yaml")
