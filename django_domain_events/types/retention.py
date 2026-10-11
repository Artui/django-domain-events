from __future__ import annotations

from enum import Enum


class Retention(Enum):
    """Delete an event as soon as it has been consumed, rather than when a
    retention window passes.

    Passed to ``@event(retention=...)``, the alternative to a ``timedelta``.
    Copied onto every event row ``fire()`` records, so the fate of an event is
    settled when it is recorded: re-declaring or deleting the class later
    changes nothing for rows already written.

    The values are what lands in ``EventRecord.delete_when``. Nothing deletes
    until ``prune_events`` runs, so a policy is only as prompt as the schedule
    running it.
    """

    SUCCEEDED = "succeeded"
    """Once every delivery has succeeded.

    A dead or orphaned delivery keeps its event under the ordinary
    ``RETENTION_DAYS`` window, so the dead letter stays inspectable and
    ``requeue_dead`` still has a row to reopen.
    """

    SETTLED = "settled"
    """Once every delivery is terminal, dead letters included.

    A dead letter goes with its event, at the next prune, so nothing is left
    for ``requeue_dead`` or the dead-letter counts to find.
    """
