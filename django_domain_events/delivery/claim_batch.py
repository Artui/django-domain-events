from __future__ import annotations

from collections.abc import Collection
from datetime import datetime, timedelta

from django.db import connections, models, transaction

from django_domain_events.declaration.registry import registry
from django_domain_events.delivery.utils import GIVEN_BACK, report_failure
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.registered_receiver import DEFAULT_LANE
from django_domain_events.utils import in_project_time, not_yet_due


def claim_batch(
    *,
    worker_id: str,
    now: datetime,
    lease: timedelta,
    limit: int,
    only_ids: list[int] | None = None,
    ignore_backoff: bool = False,
    lane: str | None = None,
    exclude_lanes: Collection[str] = (),
    exclude_ids: Collection[int] = (),
) -> list[int]:
    """Take ownership of up to ``limit`` deliveries and return their ids.

    Claims are leased, not marked: a worker that dies without acknowledging
    becomes re-claimable when its lease lapses, which is the same path as an
    ordinary retry rather than a special case.

    Rows are selected by ``available_at`` and never by primary key. A
    transaction holding a lower id can commit after one holding a higher id, so
    a row can become visible "in the past"; a high-water mark would skip it
    forever.

    ``ignore_backoff`` claims rows whose retry is still scheduled. It exists for
    the test helper, which cannot wait out a jittered hour to observe a retry.
    It never claims a row whose due time is still ahead of ``now`` - a receiver
    declared with ``due=`` - because that is not a wait the helper is skipping
    but the time the work exists for
    (``test_ignoring_the_backoff_never_ignores_the_schedule``).

    ``lane`` claims only the rows of the receivers declared in it, read from the
    registry on every call; ``"default"`` claims every row no named lane takes,
    which includes the rows of a receiver that no longer exists; None claims
    every lane. The filter is applied to the whole of the owed condition, both
    arms, so a relay never takes over another lane's lapsed claims
    (``test_a_lapsed_claim_in_a_named_lane_stays_out_of_the_default_lane``).

    ``exclude_lanes`` leaves lanes out of the claim, for a caller claiming
    several lanes in one statement that has paused one of them. The package's
    own relay and ``deliver_pending`` no longer pass it: they claim one lane at
    a time and skip a paused lane's turn instead, which is what lets each lane
    keep its own lease and batch size.
    It is each lane's own condition negated, so it covers both arms as ``lane``
    does - the rows a deferral handed back are lapsed claims, and exactly the
    ones not to take straight back
    (``test_a_paused_lane_covers_its_lapsed_claims_too``) - and excluding the
    default lane leaves only the named ones, or nothing where none is declared
    (``test_a_paused_default_lane_with_no_named_lanes_claims_nothing``). Empty,
    the claim is the one it was without it.

    ``exclude_ids`` leaves those rows out, and is how a pass keeps a row it is
    done with from coming straight back when the pass was told a ``now`` beyond
    the real clock: one its receiver answered with ``NotYet``, moved to a time
    that ``now`` has passed, or one handed to a task backend under a lease the
    real clock extended and ``now`` reads as lapsed. A pass on the real clock
    passes none, since neither is claimable again there until its time has
    really come
    (``test_a_row_rescheduled_in_place_on_the_real_clock_is_not_excluded``).
    Empty, it adds nothing to the claim: Django compiles a negated ``__in`` over
    no values to nothing at all, so the relay's claim is the one it was
    (``test_without_named_lanes_the_default_lane_sends_the_unfiltered_claim``).
    """
    from django_domain_events.models.delivery_record import DeliveryRecord

    now = in_project_time(now)
    retryable = models.Q(status__in=[DeliveryStatus.PENDING, DeliveryStatus.FAILED])
    if not ignore_backoff:
        retryable &= models.Q(available_at__lte=now)
    else:
        retryable &= ~not_yet_due(now)
    owed = retryable | models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=now)
    owed &= _in_lane(lane)
    for paused in sorted(exclude_lanes):
        owed &= ~_in_lane(paused)
    owed &= ~models.Q(pk__in=sorted(exclude_ids))

    alias = write_alias()
    connection = connections[alias]
    candidates = DeliveryRecord.objects.filter(owed).order_by("available_at", "pk")
    if only_ids is not None:
        candidates = candidates.filter(pk__in=only_ids)
    candidates = _locked(
        candidates,
        skip_locked=connection.features.has_select_for_update_skip_locked,
        for_update=connection.features.has_select_for_update,
    )
    ids: list[int] = []
    spent: list[int] = []
    while True:
        taken, dead = _claim_once(
            candidates,
            alias=alias,
            limit=limit - len(ids),
            worker_id=worker_id,
            now=now,
            lease=lease,
        )
        ids += taken
        spent += dead
        if not dead:
            break
    for row in DeliveryRecord.objects.select_related("event").filter(pk__in=spent).order_by("pk"):
        report_failure(
            row,
            status=DeliveryStatus.DEAD,
            attempt=row.attempts,
            error=row.last_error,
            error_type="a lapsed lease",
        )
    return ids


def _claim_once(
    candidates: models.QuerySet,
    *,
    alias: str,
    limit: int,
    worker_id: str,
    now: datetime,
    lease: timedelta,
) -> tuple[list[int], list[int]]:
    """Claim up to ``limit`` rows under one lock; return those taken and those dead-lettered."""
    from django_domain_events.models.delivery_record import DeliveryRecord

    given_back = in_project_time(GIVEN_BACK)
    taken: list[int] = []
    lapsed: list[int] = []
    dead: list[int] = []
    with transaction.atomic(using=alias):
        rows = candidates.values_list(
            "pk",
            "status",
            "claimed_by",
            "claimed_at",
            "lease_expires_at",
            "attempts",
            "max_attempts",
        )[:limit]
        for pk, status, claimed_by, claimed_at, expires, attempts, max_attempts in rows:
            if status != DeliveryStatus.CLAIMED or expires <= given_back:
                taken.append(pk)
            elif attempts + 1 < max_attempts:
                taken.append(pk)
                lapsed.append(pk)
            else:
                dead.append(pk)
                DeliveryRecord.objects.filter(pk=pk).update(
                    status=DeliveryStatus.DEAD,
                    attempts=attempts + 1,
                    completed_at=now,
                    last_error=(
                        f"Lease lapsed: {claimed_by} claimed this row at "
                        f"{claimed_at.isoformat()} and recorded no outcome before its lease "
                        f"ran out at {expires.isoformat()}. That was attempt {attempts + 1} "
                        f"of {max_attempts}."
                    ),
                )
        claim = {
            "status": DeliveryStatus.CLAIMED,
            "claimed_by": worker_id,
            "claimed_at": now,
            "lease_expires_at": now + lease,
        }
        fresh = [pk for pk in taken if pk not in lapsed]
        if fresh:
            DeliveryRecord.objects.filter(pk__in=fresh).update(**claim)
        if lapsed:
            DeliveryRecord.objects.filter(pk__in=lapsed).update(
                attempts=models.F("attempts") + 1, **claim
            )
    return taken, dead


def _in_lane(lane: str | None) -> models.Q:
    """The rows a relay serving ``lane`` may claim, as one condition."""
    if lane is None:
        return models.Q()
    if lane == DEFAULT_LANE:
        return ~models.Q(receiver_key__in=registry.receiver_keys_in_named_lanes())
    return models.Q(receiver_key__in=registry.receiver_keys_in_lane(lane))


def _locked(queryset: models.QuerySet, *, skip_locked: bool, for_update: bool) -> models.QuerySet:
    """Lock the candidate rows as strongly as the backend allows."""
    if skip_locked:
        return queryset.select_for_update(skip_locked=True)
    if for_update:
        return queryset.select_for_update()
    return queryset
