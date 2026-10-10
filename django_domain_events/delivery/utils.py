"""Helpers shared by the delivery modules that are not themselves exported."""

from __future__ import annotations

from collections.abc import Callable

from django_domain_events.declaration.registry import registry
from django_domain_events.settings import setting
from django_domain_events.types.registered_receiver import DEFAULT_LANE

OnDeferral = Callable[[str, float], None]
"""Told the lane and the requested delay, in seconds, of a deferral that did
not count and was recorded. The relay pauses that lane on hearing it."""


def claim_size(batch_size: int | None) -> int:
    """The rows one claim takes: ``batch_size`` if given, else ``BATCH_SIZE``.

    Shared by ``run_relay`` and ``deliver_pending``, which both take a size of
    their own for their claims alone - ``BATCH_SIZE`` also sizes prune batches
    and requeue chunks, and a mail relay wants a batch it can send inside one
    lease. A size below one is refused rather than run: it claims nothing on
    every pass, so a relay sized that way idles forever with work owed
    (``test_a_relay_refuses_a_batch_that_claims_nothing``).
    """
    if batch_size is None:
        return setting("BATCH_SIZE")
    if batch_size < 1:
        raise ValueError(f"batch_size must be positive, got {batch_size}")
    return batch_size


def partition_by_lane(delivery_ids: list[int], lane: str) -> tuple[list[int], list[int]]:
    """Split ids into those whose receiver is in ``lane`` and the rest, in order.

    What a worker uses on a deferral to find which of its unstarted rows to
    hand back. Shared by ``run_relay`` and ``deliver_pending`` as
    ``claim_size`` is. Membership is the registry's, as the claim reads it: a
    row whose receiver no longer exists is in the default lane, which is where
    it drains (``test_a_deleted_receivers_row_is_in_the_default_lane``).

    One query, run only on a deferral. A batch claimed for one lane needs no
    split - every row is in it - but asking is what lets a relay serving every
    lane hand back the throttled lane's rows and go on delivering the others.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    keys = dict(
        DeliveryRecord.objects.filter(pk__in=delivery_ids).values_list("pk", "receiver_key")
    )
    inside = [pk for pk in delivery_ids if _lane_of(keys.get(pk, "")) == lane]
    return inside, [pk for pk in delivery_ids if pk not in inside]


def _lane_of(receiver_key: str) -> str:
    """The lane a row is claimed in: its receiver's, or the default for none."""
    receiver = registry.receiver_for_key(receiver_key)
    return DEFAULT_LANE if receiver is None else receiver.lane
