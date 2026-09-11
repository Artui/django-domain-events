"""What a receiver is told about its own failed attempt."""

from __future__ import annotations

from dataclasses import dataclass

from django_domain_events.types.delivery_status import DeliveryStatus


@dataclass(frozen=True, slots=True)
class DeliveryFailure:
    """One failed attempt, handed to a receiver's ``on_failure`` hook.

    Carries the delivery's identity rather than the delivery row, deliberately.
    The hook runs at a moment when the row has just been written by the relay,
    and handing over a model instance invites a second write against a row
    another worker may now own.
    """

    delivery_id: int
    event_id: int
    event_name: str
    receiver_key: str

    attempt: int
    """Which attempt this was, counting from one."""

    status: DeliveryStatus
    """``FAILED`` while attempts remain, ``DEAD`` once the budget is spent. The
    hook is called for both, because "it failed again" and "it will not be
    tried again" are different things to record."""

    error: str
    """The message the relay stored, already truncated as the column is."""
