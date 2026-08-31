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
    """

    def enqueue(self, delivery_id: int) -> None:
        """Arrange for ``deliver_one(delivery_id)`` to run somewhere."""
        ...
