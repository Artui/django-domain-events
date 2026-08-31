from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

from django.db import transaction

from django_domain_events.backoff import backoff
from django_domain_events.causation import caused_by
from django_domain_events.claim_batch import claim_batch
from django_domain_events.fire import call_receiver
from django_domain_events.registry import registry
from django_domain_events.settings import get_codec, setting
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.write_alias import write_alias


def deliver_one(delivery_id: int, *, worker_id: str | None = None) -> DeliveryStatus | None:
    """Run one delivery and record its outcome. ``None`` means it was lost.

    The receiver's work and the acknowledgement commit together, so a receiver
    touching only this database is effectively once: the duplicate at-least-once
    owes you cannot be observed. Side effects outside the database are
    at-least-once, as promised.

    Every write here is a compare-and-set against the claim this call read, and
    the lease is extended to cover this one delivery before the receiver runs.
    Both exist for the same reason: a claim can lapse while its worker is still
    alive, and a worker that has lost its row must not go on to write a verdict
    over whoever legitimately took it. Losing the row returns ``None`` rather
    than raising - it is an ordinary outcome of a lease expiring, not a fault.
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    delivery = DeliveryRecord.objects.select_related("event").get(pk=delivery_id)
    fence = _Fence(delivery, worker_id)
    now = datetime.now(timezone.utc)

    if delivery.status == DeliveryStatus.CLAIMED and not fence.extend_lease(now):
        return None

    receiver = registry.receiver_for_key(delivery.receiver_key)
    if receiver is None:
        # Terminal: no amount of waiting brings back a deleted receiver, and a
        # row retrying forever is indistinguishable from a broken one.
        return fence.write(
            status=DeliveryStatus.ORPHANED,
            last_error=(
                f"No receiver is registered under {delivery.receiver_key!r}. It was "
                f"renamed, moved or deleted after this delivery was recorded."
            ),
            completed_at=now,
        )

    event_entry = registry.event_for_name(delivery.event.name)
    if event_entry is None:
        return _fail(
            fence,
            delivery,
            f"No event is registered under {delivery.event.name!r}, so its "
            f"payload cannot be rebuilt.",
        )

    try:
        event = get_codec().decode(
            event_entry.event_class, delivery.event.payload, delivery.event.version
        )
    except Exception as exc:
        # Terminal: it will not decode on the next attempt either, and one
        # undecodable row must not stop the other four thousand.
        return fence.write(
            status=DeliveryStatus.DEAD,
            attempts=delivery.attempts + 1,
            last_error=f"{type(exc).__name__}: {exc}"[:2000],
            completed_at=datetime.now(timezone.utc),
        )

    attempt = delivery.attempts + 1
    context = DeliveryContext(
        event_id=delivery.event.pk,
        event_name=delivery.event.name,
        attempt=attempt,
        actor_key=delivery.event.actor_key,
        scope=delivery.event.scope,
    )
    try:
        with (
            transaction.atomic(using=write_alias()),
            caused_by(delivery.event.pk, delivery.event.correlation_id),
        ):
            call_receiver(receiver.func, receiver.takes_context, event, context)
            outcome = fence.write(
                status=DeliveryStatus.SUCCEEDED,
                attempts=attempt,
                completed_at=datetime.now(timezone.utc),
            )
            if outcome is None:
                # Lost the row mid-flight. Roll the receiver's work back with
                # the acknowledgement it belongs to rather than leaving the two
                # disagreeing, and let whoever holds the claim deliver it.
                transaction.set_rollback(True, using=write_alias())
            return outcome
    except Exception as exc:
        return _fail(fence, delivery, f"{type(exc).__name__}: {exc}", attempt=attempt)


class _Fence:
    """Optimistic ownership check for one delivery's writes.

    ``claimed_at`` moves on every claim, so the pair with ``claimed_by`` is a
    fencing token: a write conditioned on both lands only while this call still
    owns the row.
    """

    def __init__(self, delivery: Any, worker_id: str | None = None) -> None:
        self.pk = delivery.pk
        # The owner this call believes it is, not the owner the row currently
        # names. A worker handed an id from a batch it claimed minutes ago may
        # have lost the row since; conditioning on what it just read would make
        # the check agree with whoever took it.
        self.claimed_by = worker_id if worker_id is not None else delivery.claimed_by
        self.claimed_at = delivery.claimed_at

    def _owned(self) -> Any:
        from django_domain_events.models.delivery_record import DeliveryRecord

        return DeliveryRecord.objects.filter(
            pk=self.pk, claimed_by=self.claimed_by, claimed_at=self.claimed_at
        )

    def extend_lease(self, now: datetime) -> bool:
        """Push the lease out to cover this delivery alone.

        A batch claim stamps one expiry across every row it took, and the relay
        then delivers them one at a time; without this the lease is a budget for
        the whole batch and runs out partway through it.
        """
        lease = timedelta(seconds=setting("LEASE_SECONDS"))
        return bool(self._owned().update(lease_expires_at=now + lease))

    def write(self, **fields: Any) -> DeliveryStatus | None:
        """Record an outcome, or return ``None`` if the row is no longer ours."""
        if not self._owned().update(**fields):
            return None
        return fields["status"]


def _fail(
    fence: _Fence, row: Any, message: str, attempt: int | None = None
) -> DeliveryStatus | None:
    """Record a failed attempt, dead-lettering once the budget is spent."""
    now = datetime.now(timezone.utc)
    attempts = attempt if attempt is not None else row.attempts + 1
    exhausted = attempts >= row.max_attempts
    return fence.write(
        status=DeliveryStatus.DEAD if exhausted else DeliveryStatus.FAILED,
        attempts=attempts,
        last_error=message[:2000],
        completed_at=now if exhausted else None,
        available_at=now
        + backoff(
            attempts,
            base=setting("BACKOFF_BASE_SECONDS"),
            cap=setting("BACKOFF_CAP_SECONDS"),
            jitter=random.random(),
        ),
    )


def deliver_pending(
    limit: int | None = None,
    *,
    worker_id: str = "deliver_pending",
    ignore_backoff: bool = False,
) -> dict[DeliveryStatus, int]:
    """Claim and deliver what is owed, and report the outcome.

    ``limit=None`` means everything owed, claimed in batches until none is left;
    a number caps it at that many. Claims go through the same leased path the
    relay uses, so a pass here and a running relay do not hand the same row to
    two receivers - on a backend with row locking. SQLite has none, so two
    concurrent passes there can both take the same row.
    """
    lease = timedelta(seconds=setting("LEASE_SECONDS"))
    batch_size = setting("BATCH_SIZE")

    counts: dict[DeliveryStatus, int] = {}
    while True:
        ids = claim_batch(
            worker_id=worker_id,
            now=datetime.now(timezone.utc),
            lease=lease,
            limit=limit if limit is not None else batch_size,
            ignore_backoff=ignore_backoff,
        )
        for delivery_id in ids:
            outcome = deliver_one(delivery_id, worker_id=worker_id)
            if outcome is not None:
                counts[outcome] = counts.get(outcome, 0) + 1
        if limit is not None or not ids:
            return counts
