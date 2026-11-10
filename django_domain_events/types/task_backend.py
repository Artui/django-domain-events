from __future__ import annotations

from typing import Protocol


class TaskBackend(Protocol):
    """Where a durable receiver's code runs when the relay hands it off."""

    def enqueue(self, delivery_id: int, claimed_by: str, claimed_at: str) -> None:
        """Arrange for the delivery to run somewhere, carrying its claim."""
        ...
