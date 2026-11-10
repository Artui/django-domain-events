from __future__ import annotations


class PermanentFailure(Exception):
    """Raised by a durable receiver to say this delivery can never succeed."""
