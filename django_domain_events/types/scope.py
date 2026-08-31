from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class Scope:
    """The ambient facts captured onto every event fired inside a block.

    Frozen, and nested blocks build a new one rather than mutating: a scope that
    could be edited from inside would let a receiver rewrite the attribution of
    events fired after it.
    """

    actor_key: str = ""
    actor_label: str = ""
    actor_pk: Any = None
    """Kept apart from ``actor_key`` so the foreign key can be set without
    parsing an identity string back into a primary key."""

    correlation_id: UUID | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def merged(self, other: Scope) -> Scope:
        """Layer another scope over this one, keeping what it does not set."""
        return replace(
            self,
            actor_key=other.actor_key or self.actor_key,
            actor_label=other.actor_label or self.actor_label,
            actor_pk=other.actor_pk if other.actor_pk is not None else self.actor_pk,
            correlation_id=other.correlation_id or self.correlation_id,
            data={**self.data, **other.data},
        )
