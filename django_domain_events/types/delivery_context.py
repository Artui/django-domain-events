from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    """Delivery metadata, passed only to receivers declaring ``takes_context``.

    Frozen data read off the event row, never a live handle: a durable delivery
    can run in another process hours after the scope that produced it has gone.
    """

    event_id: int
    event_name: str
    attempt: int
    actor_key: str
    #: The human-readable name of the actor, as ``attributed()`` was given it.
    #: Beside ``actor_key`` because the two answer different questions:
    #: ``actor_key`` is ``auth.User:1``, which is right for joining and wrong
    #: for the line an audit reader sees, and that difference is why they are
    #: two fields on the event row rather than one. A receiver writing an audit
    #: entry is the caller that wants the label, and it used to be the one
    #: place the label did not reach.
    actor_label: str
    scope: dict[str, Any]
