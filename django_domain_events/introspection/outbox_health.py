from __future__ import annotations

from datetime import datetime

from django.db import models
from django.db.models.functions import Coalesce
from django.utils import timezone

from django_domain_events.declaration.registry import registry
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.types.lane_backlog import LaneBacklog
from django_domain_events.types.lane_pause import LanePause
from django_domain_events.types.outbox_health import OutboxHealth
from django_domain_events.types.receiver_backlog import ReceiverBacklog
from django_domain_events.utils import in_project_time, not_yet_due, owed


def outbox_health(*, now: datetime | None = None) -> OutboxHealth:
    """How far behind the outbox is: owed rows by receiver and lane, scheduled ones apart."""
    from django_domain_events.models.delivery_record import DeliveryRecord
    from django_domain_events.models.lane_pause_record import LanePauseRecord

    moment = in_project_time(now) if now is not None else timezone.now()
    is_owed = owed()
    is_dead = models.Q(status=DeliveryStatus.DEAD)
    is_claimed = models.Q(status=DeliveryStatus.CLAIMED)
    waiting = not_yet_due(moment)
    due = ~waiting
    owed_since = Coalesce("due_at", "event__recorded_at")
    owed_rows = DeliveryRecord.objects.filter(is_owed)

    totals = owed_rows.aggregate(
        owed=models.Count("pk", filter=due),
        scheduled=models.Count("pk", filter=waiting),
        claimed=models.Count("pk", filter=is_claimed),
        oldest=models.Min(owed_since, filter=due),
        lapsed=models.Count(
            "pk",
            filter=models.Q(status=DeliveryStatus.CLAIMED, lease_expires_at__lt=moment),
        ),
    )
    dead_total = DeliveryRecord.objects.filter(is_dead).count()

    per_receiver = (
        DeliveryRecord.objects.filter(is_dead | is_owed)
        .values("receiver_key")
        .annotate(
            owed=models.Count("pk", filter=is_owed & due),
            dead=models.Count("pk", filter=is_dead),
            scheduled=models.Count("pk", filter=is_owed & waiting),
            claimed=models.Count("pk", filter=is_claimed),
            oldest=models.Min(owed_since, filter=is_owed & due),
        )
    )
    backlogs = [
        ReceiverBacklog(
            key=row["receiver_key"],
            owed=row["owed"],
            dead=row["dead"],
            oldest_owed_at=row["oldest"],
            scheduled=row["scheduled"],
            claimed=row["claimed"],
        )
        for row in per_receiver
    ]
    backlogs.sort(key=lambda b: (-b.owed, -b.dead, b.key))

    members: dict[str, list[ReceiverBacklog]] = {lane: [] for lane in registry.lanes()}
    for backlog in backlogs:
        members.setdefault(registry.lane_for_key(backlog.key), []).append(backlog)

    pauses: dict[str, list[LanePause]] = {}
    for lane, worker_id, paused_at, until in (
        LanePauseRecord.objects.filter(until__gt=moment)
        .order_by("worker_id")
        .values_list("lane", "worker_id", "paused_at", "until")
    ):
        pauses.setdefault(lane, []).append(
            LanePause(worker_id=worker_id, paused_at=paused_at, until=until)
        )
    for lane in pauses:
        members.setdefault(lane, [])

    lanes = [
        _lane_backlog(lane, entries, pauses.get(lane, [])) for lane, entries in members.items()
    ]
    lanes.sort(key=lambda b: (-b.owed, -b.dead, b.lane))

    return OutboxHealth(
        owed=totals["owed"],
        claimed=totals["claimed"],
        dead=dead_total,
        oldest_owed_at=totals["oldest"],
        lapsed_leases=totals["lapsed"],
        receivers=tuple(backlogs),
        scheduled=totals["scheduled"],
        lanes=tuple(lanes),
    )


def _lane_backlog(
    lane: str, receivers: list[ReceiverBacklog], pauses: list[LanePause]
) -> LaneBacklog:
    """One lane's receivers added up: counts summed, the earliest age kept."""
    return LaneBacklog(
        lane=lane,
        owed=sum(r.owed for r in receivers),
        scheduled=sum(r.scheduled for r in receivers),
        claimed=sum(r.claimed for r in receivers),
        dead=sum(r.dead for r in receivers),
        oldest_owed_at=min(
            (r.oldest_owed_at for r in receivers if r.oldest_owed_at is not None),
            default=None,
        ),
        pauses=tuple(pauses),
    )
