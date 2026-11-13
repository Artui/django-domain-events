from __future__ import annotations

from collections.abc import Callable


class LaneRate:
    """Spaces a lane's starts at least 1/per_second apart on an injected clock."""

    def __init__(self, per_second: float, monotonic: Callable[[], float]) -> None:
        if not per_second > 0:
            raise ValueError(f"A lane rate must be greater than 0, got {per_second!r}.")
        self._interval = 1 / per_second
        self._monotonic = monotonic
        self._last: float | None = None

    def wait_time(self) -> float:
        if self._last is None:
            return 0.0
        return max(0.0, self._last + self._interval - self._monotonic())

    def started(self) -> None:
        self._last = self._monotonic()
