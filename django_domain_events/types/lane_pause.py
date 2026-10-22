from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class LanePause:
    """One relay holding one lane paused after a deferral, as it recorded it.

    A pause governs only the relay that heard the deferral, and that relay
    alone writes and clears it. A relay that died without returning leaves its
    pause showing until ``until`` passes, so compare ``worker_id`` against the
    processes running before reading this as a lane nobody serves.
    """

    worker_id: str
    """The ``worker_id`` of the relay holding the pause: ``host:pid`` under
    ``deliver_events`` unless ``--worker-id`` names another."""

    paused_at: datetime
    """When the relay heard the deferral that paused the lane."""

    until: datetime
    """When the relay takes the lane up again: it claims the lane at its first
    reading of the clock at or past this
    (``test_the_pause_ends_when_the_requested_delay_has_passed_on_the_injected_clock``)."""
