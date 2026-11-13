from __future__ import annotations

from collections.abc import Collection
from datetime import datetime

from django_domain_events.delivery.utils import GIVEN_BACK
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.utils import in_project_time


def hand_back(delivery_ids: Collection[int], *, worker_id: str, claimed_at: datetime) -> int:
    """Give back claimed rows this worker has not started, and return how many."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    return DeliveryRecord.objects.filter(
        pk__in=list(delivery_ids),
        status=DeliveryStatus.CLAIMED,
        claimed_by=worker_id,
        claimed_at=claimed_at,
    ).update(lease_expires_at=in_project_time(GIVEN_BACK))
