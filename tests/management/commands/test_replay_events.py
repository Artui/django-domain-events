from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command
from django.db import transaction

from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.fire import fire
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_reports_what_it_reopened(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()

    out = StringIO()
    call_command("replay_events", str(event_id), stdout=out)
    assert "reopened: 2, added: 0" in out.getvalue()


def test_it_can_be_narrowed_to_one_receiver(order: OrderPlaced, record: list[str]) -> None:
    with transaction.atomic():
        event_id = fire(order)
    drain_outbox()

    out = StringIO()
    call_command(
        "replay_events", str(event_id), "--receiver", "testapp.durable_receiver", stdout=out
    )
    assert "reopened: 1" in out.getvalue()
