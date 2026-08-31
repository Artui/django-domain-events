"""Tests mirroring ``django_domain_events/management/commands/deliver_events.py``."""

from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
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


def test_once_is_required_rather_than_defaulted() -> None:
    """A command that looks like a daemon would be one, and a continuous relay
    needs the leased claim that makes two workers safe. Requiring the flag now
    means the flag cannot silently change meaning when that lands."""
    with pytest.raises(CommandError, match="leased claim"):
        call_command("deliver_events")
