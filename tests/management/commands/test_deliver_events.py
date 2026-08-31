"""Tests mirroring ``django_domain_events/management/commands/deliver_events.py``."""

from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connection, transaction

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
    same row to two receivers on every pass.

    Guarded on the backend rather than left to run everywhere. Without ``--passes``
    the command relays forever, so on a backend that does *not* refuse this call
    never returns -- and the version of this test that did not skip hung the whole
    Postgres suite until it was killed. A test that passes only because the
    default backend refuses is a test that passes for the wrong reason.
    """
    if connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend supports skipped locks, so the relay would run")
    with pytest.raises(RuntimeError, match="SKIP LOCKED"):
        call_command("deliver_events")


def test_the_relay_runs_for_a_bounded_number_of_passes(
    order: OrderPlaced, record: list[str]
) -> None:
    """The relay path on a backend that supports it, kept finite by --passes."""
    if not connection.features.has_select_for_update_skip_locked:
        pytest.skip("this backend cannot skip locks, so the relay refuses")
    with transaction.atomic():
        fire(order)
    out = StringIO()
    call_command("deliver_events", "--passes", "1", stdout=out)
    assert "succeeded: 2" in out.getvalue()
