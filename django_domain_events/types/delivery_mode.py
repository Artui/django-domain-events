"""What a receiver promises about the firing transaction."""

from __future__ import annotations

from enum import Enum


class DeliveryMode(Enum):
    """Timing and guarantee, declared per receiver.

    This is one of two independent knobs. ``DeliveryMode`` is *when* a receiver
    runs relative to the transaction that fired the event; where its code runs is
    a separate question, and only meaningful for :attr:`DURABLE`. Collapsing the
    two into one enum is what makes event libraries confusing, because a queue is
    only ever an answer to the second question.
    """

    INLINE = "inline"
    """Runs inside the firing transaction, and may veto it by raising.

    Needs no durability, and that is not an omission: its failure mode is a
    rollback, so if the process dies the transaction dies with it and nothing is
    owed. It is the only mode that can abort the business operation.

    If a handler *must* be able to veto, it is arguably a call and not an event.
    The mode exists because the need is real; the smell is worth naming.
    """

    ON_COMMIT = "on_commit"
    """Runs after commit, in the firing process, best-effort.

    Not recoverable: a process death between commit and the receiver finishing
    loses the work with no record that it was owed. For cache busts and local
    metrics, where losing one is cheaper than the row it would cost to guarantee
    it.
    """

    DURABLE = "durable"
    """Runs after commit, at-least-once, retried, with a row recording the debt.

    The default, and the reason this package exists: the delivery row is written
    inside the firing transaction, so the obligation exists if and only if the
    business change committed.
    """
