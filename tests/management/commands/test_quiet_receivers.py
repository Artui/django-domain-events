"""Tests mirroring ``management/commands/quiet_receivers.py``."""

from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from tests.testapp.events import Eagerly, OrderPlaced, SlowWork

pytestmark = pytest.mark.django_db


def _run(*args: str) -> str:
    out = StringIO()
    call_command("quiet_receivers", *args, stdout=out)
    return out.getvalue()


def test_it_lists_receivers_that_have_never_run() -> None:
    output = _run()
    assert "testapp.durable_receiver\ttestapp.OrderPlaced\tnever" in output


def test_a_receiver_that_has_run_drops_out(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
        fire(Eagerly(value=1))
        fire(SlowWork(value=1))
    drain_outbox()
    assert "Every durable receiver has run inside the window." in _run()


def test_the_window_can_be_narrowed(order: OrderPlaced, record: list[str]) -> None:
    """Zero days is an operator asking "did anything run just now", and it must
    not be read as "no window"."""
    with transaction.atomic():
        fire(order)
        fire(Eagerly(value=1))
    drain_outbox()
    assert "testapp.durable_receiver" in _run("--days", "0")


def test_a_receiver_that_has_run_reports_when(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    drain_outbox()
    line = next(line for line in _run("--days", "0").splitlines() if "durable_receiver" in line)
    assert line.count("\t") == 2
    assert "never" not in line
