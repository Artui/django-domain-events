from __future__ import annotations

from typing import Any, Protocol, TypeVar

E = TypeVar("E")


class PayloadCodec(Protocol):
    """How an event instance becomes a payload, and comes back.

    A seam because the two halves cost differently: encoding needs nothing this
    package does not have, while decoding a nested payload back into a frozen
    dataclass is worth a dependency. Every codec consumes an ordinary frozen
    dataclass, so the choice never reaches how events are declared.
    """

    def encode(self, event: object) -> dict[str, Any]: ...

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E: ...
