from __future__ import annotations


class Moot(Exception):
    """Raised by a durable receiver that found nothing to do: the row is settled CANCELLED,
    its writes rolled back, with ``moot: <reason>`` as ``last_error``, not as a success and
    not as a failure. Read by the relay alone: from INLINE or ON_COMMIT it is any exception."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)
