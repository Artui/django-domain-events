from __future__ import annotations

from datetime import timedelta


def backoff(attempt: int, *, base: float, cap: float, jitter: float) -> timedelta:
    """How long to wait before attempt number ``attempt`` may be retried."""
    if attempt < 1:
        raise ValueError(f"attempt is 1-based, got {attempt}")
    ceiling = min(cap, base * (2 ** (attempt - 1)))
    return timedelta(seconds=ceiling * jitter)
