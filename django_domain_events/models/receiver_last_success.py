from __future__ import annotations

from django.db import models


class ReceiverLastSuccess(models.Model):
    """The newest success of one receiver among the deliveries the prune has
    deleted.

    ``quiet_receivers()`` reads a receiver's last success off its delivery
    rows, so once the prune has deleted them - which an event declared to
    delete on consumption does within a sweep - the receiver would read as
    never having run. The prune writes the newest ``succeeded_at`` of what it
    deletes here, in the same transaction as the delete, and
    ``quiet_receivers()`` takes the later of this and the live rows.

    Written only by the prune, never on the delivery path: every delivery of a
    receiver would update this one row, so a 20,000-row fan-out would queue
    every completion on its lock.

    One row per receiver key that has ever had a delivery pruned. A key whose
    receiver has since been deleted keeps its row, which costs one row and is
    read by nothing, since the reader asks only about declared receivers.
    """

    receiver_key = models.CharField(max_length=255, unique=True)
    last_succeeded_at = models.DateTimeField()

    class Meta:
        verbose_name = "receiver last success"
        verbose_name_plural = "receiver last successes"

    def __str__(self) -> str:
        return f"{self.receiver_key} @ {self.last_succeeded_at.isoformat()}"
