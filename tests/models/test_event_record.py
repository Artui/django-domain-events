"""Tests mirroring ``django_domain_events/models/event_record.py``."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from django.db import models

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


def test_a_new_event_follows_the_ordinary_retention_window() -> None:
    """Both retention columns default to "no policy of its own", so an event
    recorded before they existed, and one fired by a declaration that says
    nothing about retention, are pruned by RETENTION_DAYS alone."""
    row = EventRecord.objects.create(
        name="testapp.OrderPlaced",
        version=1,
        payload={},
        occurred_at=datetime(2026, 8, 31, tzinfo=timezone.utc),
    )
    row.refresh_from_db()
    assert row.retention_seconds is None
    assert row.delete_when == ""


def test_only_events_carrying_a_policy_of_their_own_are_in_the_retention_index() -> None:
    """Partial, so the ordinary event - nearly every row - costs the index
    nothing, and the sweep that looks for events with a policy of their own
    reads only those."""
    [index] = [i for i in EventRecord._meta.indexes if i.name == "dde_own_retention"]
    assert tuple(index.fields) == ("recorded_at",)
    assert index.condition == models.Q(retention_seconds__isnull=False) | ~models.Q(delete_when="")
