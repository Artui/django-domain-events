from __future__ import annotations

from django.db import models


class ReceiverLastSuccess(models.Model):
    """The newest success of one receiver among the deliveries the prune has deleted."""

    receiver_key = models.CharField(max_length=255, unique=True)
    last_succeeded_at = models.DateTimeField()

    class Meta:
        verbose_name = "receiver last success"
        verbose_name_plural = "receiver last successes"

    def __str__(self) -> str:
        return f"{self.receiver_key} @ {self.last_succeeded_at.isoformat()}"
