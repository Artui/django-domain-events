from __future__ import annotations


class RetryAfter(Exception):
    """Raised by a durable receiver to say when its next attempt should run.

    The relay schedules the retry ``seconds`` from now instead of drawing it from
    the backoff curve. This is the one case where the other side has told us the
    answer: a ``429`` or ``503`` carrying ``Retry-After: 120`` is a destination
    saying when it will be ready, and the exponential curve is a guess. Guessing
    against a rate limiter means arriving early and being refused again, which
    the curve then punishes by waiting longer than was asked.

    **By default it consumes an attempt.** A retry that did not count would let
    a destination keep a row alive forever by answering ``429`` to every
    attempt, and nothing would ever declare it dead. Counting it keeps every
    delivery bounded by the budget it was fired with; a receiver expecting to
    be rate limited widens ``max_attempts`` at its declaration. On the last
    attempt of the budget the row is dead-lettered like any other failure, and
    the requested time is moot.

    **``counts=False`` is a deferral instead**, for a destination limiting how
    much it accepts rather than refusing this row: a throttle, a quota. The
    attempt is not counted and ``max_attempts`` is not spent; what ends the
    delivery is ``give_up_after=`` on the receiver, a bound in time measured
    from when the row became owed, past which the next deferral dead-letters
    it. The next attempt runs between ``seconds`` and twice that from now -
    never earlier than asked, spread so a throttled burst does not come back as
    one wave - and the relay that ran it pauses the receiver's lane for
    ``seconds`` and hands back the rest of its batch rather than spending a
    call on each row to learn what the first already said.

    A receiver raising ``counts=False`` without declaring ``give_up_after`` has
    nothing that would ever end the delivery, so the deferral is counted like
    any other ``RetryAfter`` - no jitter, no pause - and a warning naming the
    receiver is logged once per process.

    **It is capped** at ``MAX_RECEIVER_RETRY_DELAY_SECONDS``, and a request past
    the cap is clamped with a warning naming both numbers. The cap is not
    ``BACKOFF_CAP_SECONDS``, which bounds a curve: clamping a legitimate one-hour
    ``Retry-After`` to a backoff ceiling would hammer a destination that asked to
    be left alone, which is the opposite of the feature.

    Everything else about a failed attempt is unchanged: the attempt is counted,
    the message is stored as ``last_error``, and ``on_failure`` is called with
    ``FAILED``. A deferral that does not count is recorded the same way, with
    the row ``FAILED`` and ``on_failure`` told the attempt number the receiver
    was given - which the next run is given again, since nothing was spent.

    A negative delay is refused where it is constructed. The relay would record
    that refusal as an ordinary failure on the ordinary curve, with a message
    saying why, rather than scheduling an attempt in the past.
    """

    def __init__(self, seconds: float, reason: str = "", *, counts: bool = True) -> None:
        # One comparison rather than two: NaN compares false with everything,
        # so ``seconds < 0`` would wave it through and the relay would then
        # try to build a timedelta from it.
        if not seconds >= 0:
            raise ValueError(f"RetryAfter needs a delay of zero seconds or more, got {seconds!r}")
        self.seconds = float(seconds)
        self.reason = reason
        self.counts = counts
        super().__init__(reason or f"retry requested in {self.seconds:g}s")
