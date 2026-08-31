"""When a receiver runs relative to the firing transaction."""

from __future__ import annotations

from enum import Enum


class DeliveryMode(Enum):
    """Timing and guarantee, declared per receiver.

    Timing is separate from where a receiver's code runs; a queue is only ever an
    answer to the second question.
    """

    INLINE = "inline"
    """Inside the firing transaction, and free to abort it by raising.

    Needs no durability: its failure mode is a rollback, so nothing can be owed.
    """

    ON_COMMIT = "on_commit"
    """After commit, in the firing process, best effort. Not recoverable."""

    DURABLE = "durable"
    """After commit, at-least-once, retried, with a row recording the debt."""
