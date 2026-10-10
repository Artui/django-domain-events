"""Tests mirroring ``django_domain_events/models/receiver_last_success.py``."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from django.db import IntegrityError, transaction

from django_domain_events.models.receiver_last_success import ReceiverLastSuccess

pytestmark = pytest.mark.django_db

AT = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)


def test_it_identifies_itself_by_receiver_and_time() -> None:
    row = ReceiverLastSuccess.objects.create(receiver_key="shop.email", last_succeeded_at=AT)
    assert str(row) == "shop.email @ 2026-10-01T09:00:00+00:00"


def test_one_row_per_receiver() -> None:
    """The prune's insert relies on the conflict to leave an existing row for
    its conditional update, so a second row per key would make the reader's
    answer depend on which one it found."""
    ReceiverLastSuccess.objects.create(receiver_key="shop.email", last_succeeded_at=AT)
    with pytest.raises(IntegrityError), transaction.atomic():
        ReceiverLastSuccess.objects.create(receiver_key="shop.email", last_succeeded_at=AT)
