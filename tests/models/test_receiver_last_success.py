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
    """The conflict leaves one row per key, so a reader's answer is well defined."""
    ReceiverLastSuccess.objects.create(receiver_key="shop.email", last_succeeded_at=AT)
    with pytest.raises(IntegrityError), transaction.atomic():
        ReceiverLastSuccess.objects.create(receiver_key="shop.email", last_succeeded_at=AT)
