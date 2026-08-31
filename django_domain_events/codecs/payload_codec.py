"""The seam between a declared event class and the JSON column it lands in."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

E = TypeVar("E")


class PayloadCodec(Protocol):
    """How an event instance becomes a payload, and comes back.

    A seam rather than a hard choice, because the two halves have very different
    costs. Encoding needs nothing this package does not already have: stdlib
    ``asdict`` plus Django's own ``DjangoJSONEncoder`` already handles every
    scalar a domain event carries. Decoding a nested payload back into a frozen
    dataclass is the part worth a dependency, and the part a consumer may
    reasonably want to choose.

    Every codec consumes an ordinary frozen dataclass, so which one is configured
    never reaches how a consumer declares an event.
    """

    def encode(self, event: object) -> dict[str, Any]:
        """Return a JSON-safe dict for the event instance."""
        ...

    def decode(self, event_class: type[E], payload: dict[str, Any], version: int) -> E:
        """Rebuild an instance, raising for a payload this codec cannot honour.

        ``version`` is the schema version recorded on the row. A codec that does
        not migrate reads it only to name it in an error, which is still worth
        more than a decode failure that cannot say which vintage it choked on.
        """
        ...
