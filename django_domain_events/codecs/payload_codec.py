from __future__ import annotations

from typing import Any, Protocol, TypeVar

E = TypeVar("E")


class PayloadCodec(Protocol):
    """How an event becomes a payload and back: decoding is the half worth swapping."""

    def encode(self, event: object) -> dict[str, Any]: ...

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E: ...
