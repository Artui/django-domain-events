from __future__ import annotations

from typing import NamedTuple


class Pruned(NamedTuple):
    """What one prune deleted: events, and the delivery rows that went with them."""

    events: int
    deliveries: int
