from __future__ import annotations


class RetryAfter(Exception):
    """Raised by a durable receiver to say when its next attempt should run."""

    def __init__(self, seconds: float, reason: str = "", *, counts: bool = True) -> None:
        # One comparison: NaN compares false with everything, so seconds < 0 would pass it.
        if not seconds >= 0:
            raise ValueError(f"RetryAfter needs a delay of zero seconds or more, got {seconds!r}")
        if not counts and not seconds > 0:
            raise ValueError(
                f"RetryAfter(counts=False) needs a delay above zero seconds, got {seconds!r}"
            )
        self.seconds = float(seconds)
        self.reason = reason
        self.counts = counts
        super().__init__(reason or f"retry requested in {self.seconds:g}s")
