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
    """None when nothing is owed. Its age is the one number worth alerting on:
    it goes up when the relay is down, when a receiver is failing, and when the
    queue is simply longer than the workers can drain, and those are the three
    things an operator needs to hear about."""

    lapsed_leases: int
    """Claimed rows whose lease has expired. Steady non-zero means workers are
    dying mid-delivery, or a receiver outruns its lease and has its work
    thrown away every time - see lease_seconds= on the receiver."""

    receivers: tuple[ReceiverBacklog, ...]
    """Only receivers with something owed or dead, worst backlog first."""
