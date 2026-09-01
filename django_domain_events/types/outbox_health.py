from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from django_domain_events.types.receiver_backlog import ReceiverBacklog


@dataclass(frozen=True, slots=True)
class OutboxHealth:
    """Whether the outbox is keeping up, in one value.

    Counts and one timestamp rather than rates: a rate needs two samples and a
    place to keep the first, which is a metrics system's job. This is what that
    system scrapes.
    """

    owed: int
    """Deliveries not yet settled: pending, failed or claimed."""

    claimed: int
    dead: int
    oldest_owed_at: datetime | None
    """When the oldest still-owed delivery's event was **recorded**, or None
    when nothing is owed.

    Not when it next becomes due: a failed delivery has its ``available_at``
    pushed into the future by the backoff, so an alert written against that
    reads a *negative* age exactly while a receiver is failing. This rises
    monotonically for as long as work sits undone, which is what a threshold
    needs.

    A receiver that fails all the way to dead leaves the owed set entirely, so
    watch ``dead`` alongside it. One number does not cover both."""

    lapsed_leases: int
    """Claimed rows whose lease has expired. Steady non-zero means workers are
    dying mid-delivery, or a receiver outruns its lease and has its work
    thrown away every time - see lease_seconds= on the receiver."""

    receivers: tuple[ReceiverBacklog, ...]
    """Only receivers with something owed or dead, worst backlog first."""
