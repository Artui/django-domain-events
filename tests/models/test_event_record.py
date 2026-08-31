"""Tests mirroring ``django_domain_events/models/event_record.py``."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from django_domain_events.models.event_record import EventRecord

pytestmark = pytest.mark.django_db


def test_it_identifies_itself_by_name_and_id() -> None:
    """What an operator sees in the admin and in a shell, where the name matters
    more than the primary key."""
    row = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
    )
    assert str(row) == f"testapp.OrderPlaced#{row.pk}"


def test_the_actor_is_optional() -> None:
    """Plenty of things that fire events are not users: a relay, a cron, a
    management command, a peer service."""
    row = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
        actor_key="system:relay",
        actor_label="the relay",
    )
    assert row.actor is None
    assert row.actor_key == "system:relay"
