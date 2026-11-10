from __future__ import annotations


class PayloadUpgradeFailed(Exception):
    """An event class's ``upgrade`` hook raised while migrating an old row."""
