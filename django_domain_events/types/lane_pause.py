from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class LanePause:
    """One recorded pause of a lane: a relay's after a deferral, or an operator's.

    Every delivery path serving the lane, a relay and ``deliver_pending()``
    alike, reads these records and claims nothing in it while any of them
    holds, whoever wrote it. A relay writes its own when a
    receiver defers and deletes it when the pause ends or it stops;
    ``pause_lane`` writes one under the ``worker_id`` ``operator``, and
    ``resume_lane`` deletes every one of a lane. A relay that died without
    returning leaves its record behind, and it holds the lane on every path
    until ``until`` passes, so before waiting it out, compare ``worker_id`` and
    ``paused_at`` against the processes running and lift a leftover with
    ``resume_lane``.
    """

    worker_id: str
    """The ``worker_id`` of the relay holding the pause, ``host:pid`` under
    ``deliver_events`` unless ``--worker-id`` names another, or ``operator`` for a
    ``pause_lane``."""

    paused_at: datetime
    """When the relay heard the deferral, or the operator ran ``pause_lane``."""

    until: datetime
    """When the pause ends: a relay claims the lane at its first
    reading of the clock at or past this
    (``test_the_pause_ends_when_the_requested_delay_has_passed_on_the_injected_clock``)."""
