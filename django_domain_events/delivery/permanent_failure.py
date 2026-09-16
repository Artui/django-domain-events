from __future__ import annotations


class PermanentFailure(Exception):
    """Raised by a durable receiver to say this delivery can never succeed.

    The relay dead-letters the row on the attempt that raised it, instead of
    spending the rest of the attempt budget first. The budget is otherwise the
    only thing that can end a delivery, so a receiver that already knows the
    destination is gone - a ``410 Gone``, a deleted account, a revoked
    credential - would go on retrying for hours against something that has told
    it, in so many words, that it will never accept another.

    An exception rather than a return value, and that is not a style choice. A
    receiver returns ``None`` by contract, and every mode calls it the same way,
    so a return convention would have to be invented for all three and would be
    silently ignored by two of them. Raising already means "this attempt
    failed"; this narrows it to "and every later one would too".

    Only the relay reads it. An ``INLINE`` receiver raising it fails the caller's
    transaction exactly as any other exception does, and an ``ON_COMMIT`` one has
    it logged: neither has a row to dead-letter.

    The row is recorded as ``DEAD`` with the attempt that raised it, and an
    ``on_failure`` hook sees ``DEAD``, exactly as it would had the budget run out.
    A consumer's failure log therefore needs no second case to recognise it.

    Beside ``deliver`` rather than at the package root, because that is the one
    module that reads it. It imports nothing, so it cannot close a cycle.
    """
