from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from django_domain_events.types.lane_backlog import LaneBacklog
from django_domain_events.types.receiver_backlog import ReceiverBacklog


@dataclass(frozen=True, slots=True)
class OutboxHealth:
    """Whether the outbox is keeping up, in one value.

    Counts and one timestamp rather than rates: a rate needs two samples and a
    place to keep the first, which is a metrics system's job. This is what that
    system scrapes.
    """

    owed: int
    """Deliveries due and not yet settled: pending, failed or claimed, with
    their due time come. A delivery scheduled for later is counted in
    ``scheduled`` instead, because it is work that has not arrived."""

    claimed: int
    dead: int
    oldest_owed_at: datetime | None
    """When the oldest still-owed delivery **became owed**, or None when
    nothing is owed.

    That is its ``due_at`` where the row has one - the due time of a receiver
    declared with ``due=``, or the moment a replay or requeue reopened it - and
    its event's ``recorded_at`` otherwise. Work arrives when it becomes due, so
    a row scheduled for a fortnight ahead does not age while it waits, and one
    failing after it came due ages from its due time rather than from an event
    three days older.

    Not when it next becomes available: a failed delivery has its
    ``available_at`` pushed into the future by the backoff, so an alert written
    against that reads a *negative* age exactly while a receiver is failing.
    This rises monotonically for as long as work sits undone, which is what a
    threshold needs.

    A receiver that fails all the way to dead leaves the owed set entirely, so
    watch ``dead`` alongside it. One number does not cover both."""

    lapsed_leases: int
    """Claimed rows whose lease has expired. Steady non-zero means workers are
    dying mid-delivery, or a receiver outruns its lease and has its work
    thrown away every time - see lease_seconds= on the receiver."""

    receivers: tuple[ReceiverBacklog, ...]
    """Only receivers with something owed, dead or scheduled, worst backlog
    first."""

    scheduled: int = 0
    """Deliveries not yet settled whose due time is still ahead: the rows of a
    receiver declared with ``due=`` that are waiting for it. Not owed and not
    aged, so a backlog alert does not fire for work that is on time. Defaulted
    and last, so adding it does not break a consumer constructing one."""

    lanes: tuple[LaneBacklog, ...] = ()
    """The same backlog split by lane, with each lane's pauses. Every lane a
    relay can serve is listed, the default and each declared one, with zeros
    where nothing is owed, so a dashboard keeps one series per lane; so is a
    lane nobody declares any more while a relay still holds it paused. Worst
    backlog first, ranked as ``receivers`` is. Defaulted and last for the
    reason ``scheduled`` is."""
