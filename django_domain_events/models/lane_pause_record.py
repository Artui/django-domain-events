from __future__ import annotations

from django.db import models


class LanePauseRecord(models.Model):
    """A lane one relay has paused after a deferral, for operators to see."""

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
