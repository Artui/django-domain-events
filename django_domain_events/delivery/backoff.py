from __future__ import annotations

from datetime import timedelta


def backoff(attempt: int, *, base: float, cap: float, jitter: float) -> timedelta:
    """How long to wait before attempt number ``attempt`` may be retried.

    ``jitter`` is supplied by the caller as a value in [0, 1) rather than drawn
    here, so the schedule is a pure function of its arguments and a test can
    assert the curve instead of sampling it.

    Full jitter: the delay is drawn from the whole window up to the exponential
    ceiling, not added on top of it. Retrying a shared downstream at
    ceiling-plus-a-bit keeps every failed delivery in the same cohort, which is
    the thundering herd the backoff was meant to break up.
    """
    if attempt < 1:
        raise ValueError(f"attempt is 1-based, got {attempt}")
    ceiling = min(cap, base * (2 ** (attempt - 1)))
    return timedelta(seconds=ceiling * jitter)
