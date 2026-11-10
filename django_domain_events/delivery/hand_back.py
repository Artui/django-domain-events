from __future__ import annotations

from collections.abc import Collection
from datetime import datetime, timezone

from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.utils import in_project_time

# Strictly before any moment a claim can be made at, on any worker's clock. The
# claim's lapsed-lease arm is a strict ``lease_expires_at < now``, so a lease
# ended "now" is still held by a claim made in the same instant - which a test
# with a frozen clock, or a worker on a clock a little behind this one, is. The
# epoch also reads as what it is in an admin listing: not a lease that ran out,
# but one that was given back. Aware, and converted where it is written: a
# project on ``USE_TZ = False`` stores naive local time, and SQLite refuses an
# aware value there (``test_handing_back_writes_a_lease_the_backend_accepts``).
_GIVEN_BACK = datetime(1970, 1, 1, tzinfo=timezone.utc)


def hand_back(delivery_ids: Collection[int], *, worker_id: str, claimed_at: datetime) -> int:
    """Give back claimed rows this worker has not started, and return how many."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    return DeliveryRecord.objects.filter(
        pk__in=list(delivery_ids),
        status=DeliveryStatus.CLAIMED,
        claimed_by=worker_id,
        claimed_at=claimed_at,
    ).update(lease_expires_at=in_project_time(_GIVEN_BACK))
