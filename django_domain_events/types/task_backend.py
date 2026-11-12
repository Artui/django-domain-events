from __future__ import annotations

from typing import Protocol


class TaskBackend(Protocol):
    """Where a durable receiver's code runs when the relay hands it off.

    The relay claims the row either way; this only decides who executes it. That
    is the whole reason the execution site is a separate knob from the timing:
    a queue is only ever an answer to the second question.

    The backend may lose an enqueue without consequence. The delivery row is the
    record, so anything dropped is reclaimed when the lease lapses - which is
    what makes a lossy queue safe here, and what keeps this a small protocol.

    It may also deliver one twice, or late, which is what ``acks_late`` and a
    broker's visibility timeout do. The message carries the claim the row was
    handed off under, and the task takes the row under exactly that claim
    before running anything, so a second copy or a copy the queue held past its
    lease finds the claim gone and does nothing.
    """

    def enqueue(self, delivery_id: int, claimed_by: str, claimed_at: str) -> None:
        """Arrange for the delivery to run somewhere, carrying its claim.

        The worker must call ``deliver_one(delivery_id, claimed_by=claimed_by,
        claimed_at=claimed_at)`` with the three values exactly as given.
        ``claimed_at`` is an ISO 8601 string rather than a datetime, so all
        three are JSON and any queue can carry them unchanged. The relay passes
        the claim by keyword.
        """
        ...
