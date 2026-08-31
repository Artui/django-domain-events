"""Tests mirroring ``django_domain_events/management/commands/deliver_events.py``."""

from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command
from django.db import transaction

from django_domain_events.fire import fire
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_reports_what_it_delivered(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    out = StringIO()
    call_command("deliver_events", "--once", stdout=out)
    assert "succeeded: 2" in out.getvalue()


def test_nothing_owed_says_so() -> None:
    out = StringIO()
    call_command("deliver_events", "--once", stdout=out)
    assert out.getvalue().strip() == "Nothing owed."


def test_a_limit_is_passed_through(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        fire(order)
    out = StringIO()
    call_command("deliver_events", "--once", "--limit", "1", stdout=out)
    assert "succeeded: 1" in out.getvalue()


def test_the_relay_refuses_where_locks_cannot_be_skipped() -> None:
    """SQLite cannot express a skipped lock, so two relays on it would hand the
    same row to two receivers on every pass. Refuse at the worker rather than at
    import: import time is the test suite, which has every right to run here."""
    with pytest.raises(RuntimeError, match="SKIP LOCKED"):
        call_command("deliver_events")
