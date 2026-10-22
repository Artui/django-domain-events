from __future__ import annotations

from django.db import models


class LanePauseRecord(models.Model):
    """A lane one relay has paused after a deferral, for operators to see.

    A pause lives in the process that heard the deferral, and no relay reads
    this table: it is a window onto that process, not a way of sharing the
    pause. Sharing it would make a pause outlive the relay that heard it.

    It cannot be derived from the delivery rows instead. A deferral writes its
    row FAILED with an ``available_at`` jittered by up to twice the requested
    delay, so the pause's end cannot be read back from it, the requested delay
    is stored nowhere, and a counting ``RetryAfter`` writes a row of the same
    shape that pauses nothing.

    The relay writes a row only on a deferral, keyed on the lane and its own
    ``worker_id``, and deletes it when the pause ends at a claim or when the
    relay returns. A relay that dies without returning leaves its row until
    ``until`` passes, which is why every write also deletes every row whose
    ``until`` has passed, whoever wrote it, and why the row names its worker:
    a pause no running relay honours is told apart by comparing the
    ``worker_id`` against the running processes.

    At most one row per lane per relay, so the unique constraint is the only
    index the table needs.
    """

    lane = models.CharField(max_length=255)
    worker_id = models.CharField(max_length=255)
    paused_at = models.DateTimeField()
    until = models.DateTimeField()

    class Meta:
        constraints = [
            # What the relay's write upserts on: one relay pausing a lane again
            # moves its row rather than adding a second.
            models.UniqueConstraint(
                fields=["lane", "worker_id"],
                name="unique_lane_pause_per_worker",
            )
        ]
