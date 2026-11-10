from __future__ import annotations

from enum import Enum


class DeliveryMode(Enum):
    """Timing and guarantee, declared per receiver."""

    INLINE = "inline"
    """Inside the firing transaction, and free to abort it by raising.

    Needs no durability: its failure mode is a rollback, so nothing can be owed.
    """

    ON_COMMIT = "on_commit"
    """After commit, in the firing process, best effort. Not recoverable."""

    DURABLE = "durable"
    """After commit, at-least-once, retried, with a row recording the debt."""
