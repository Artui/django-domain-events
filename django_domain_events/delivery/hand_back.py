from __future__ import annotations

from collections.abc import Collection
from datetime import datetime, timezone

from django_domain_events.types.delivery_status import DeliveryStatus

# Strictly before any moment a claim can be made at, on any worker's clock. The
# claim's lapsed-lease arm is a strict ``lease_expires_at < now``, so a lease
# ended "now" is still held by a claim made in the same instant - which a test
# with a frozen clock, or a worker on a clock a little behind this one, is. The
# epoch also reads as what it is in an admin listing: not a lease that ran out,
# but one that was given back.
_GIVEN_BACK = datetime(1970, 1, 1, tzinfo=timezone.utc)


def hand_back(delivery_ids: Collection[int], *, worker_id: str, claimed_at: datetime) -> int:
    """Give back claimed rows this worker has not started, and return how many.

    Handing back is expiring the lease and nothing else. The row stays CLAIMED,
    so there is no earlier status to remember and restore, and the claim query's
    lapsed-lease arm takes it on the next pass of any worker - this one
    included. ``available_at`` is untouched, so the rows keep their place at the
    head of the queue.

    The rows are named rather than found, because a worker's claim also covers
    rows it *has* started and that are still CLAIMED: a row handed to a task
    backend stays CLAIMED under the relay's claim until the task takes it.
    Expiring that lease would have another worker enqueue it a second time
    while the first message is still in the queue.

    Every row is conditioned on the claim that took it, ``claimed_by`` and
    ``claimed_at`` together, which is the fencing token every other write in
    the package uses, plus its status. A row another worker has claimed since,
    one this same worker id claimed again later, and one already settled are
    all left as they are. Each condition is held by a test of its own in
    ``tests/delivery/test_hand_back.py``, because together they are one branch
    arc and coverage cannot see a deleted one:
    ``test_a_row_another_worker_has_claimed_is_not_touched`` holds
    ``claimed_by``, ``test_a_later_claim_by_the_same_worker_id_is_not_touched``
    holds ``claimed_at``, ``test_a_settled_row_is_not_touched`` holds the
    status, and ``test_only_the_named_rows_are_handed_back`` holds the ids.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    return DeliveryRecord.objects.filter(
        pk__in=list(delivery_ids),
        status=DeliveryStatus.CLAIMED,
        claimed_by=worker_id,
        claimed_at=claimed_at,
    ).update(lease_expires_at=_GIVEN_BACK)
