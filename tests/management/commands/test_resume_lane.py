from __future__ import annotations

from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.utils import timezone

from django_domain_events.models.lane_pause_record import LanePauseRecord

pytestmark = pytest.mark.django_db


def _pause(lane: str, worker_id: str) -> None:
    now = timezone.now()
    LanePauseRecord.objects.create(
        lane=lane, worker_id=worker_id, paused_at=now, until=now + timedelta(hours=1)
    )


def test_it_lifts_the_operators_and_every_relays_pause_of_the_lane_only() -> None:
    _pause("mail", "operator")
    _pause("mail", "host:1")
    _pause("mail", "host:2")
    _pause("push", "host:1")

    out = StringIO()
    call_command("resume_lane", "mail", stdout=out)

    assert out.getvalue() == "resumed lane mail: lifted 3 pauses\n"
    assert list(LanePauseRecord.objects.values_list("lane", "worker_id")) == [("push", "host:1")]


def test_a_lane_no_receiver_declares_any_more_can_still_be_resumed() -> None:
    """A renamed lane's leftover pause is shown, so it has to be liftable."""
    _pause("renamed", "host:1")

    out = StringIO()
    call_command("resume_lane", "renamed", stdout=out)

    assert out.getvalue() == "resumed lane renamed: lifted 1 pauses\n"
    assert not LanePauseRecord.objects.exists()
