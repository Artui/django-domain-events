from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any
from uuid import UUID

MAX_ACTOR_LENGTH = 255
"""Both actor columns are varchar(255). Postgres refuses a longer value with a
DataError raised inside the caller's transaction; SQLite stores it happily, so
the version matrix cannot see the difference."""


@dataclass(frozen=True, slots=True)
class Actor:
    """Who acted, as one fact rather than three columns that can disagree.

    Merged as a unit: a block that names an actor replaces all of it, and a
    block that does not inherits all of it. Merging the parts independently lets
    an inner ``actor_key="system:relay"`` sit beside an inherited ``pk`` of some
    user, and the row then says two different things about who acted.
    """

    key: str = ""
    label: str = ""
    user_pk: Any = None
    """Only ever a primary key of the swapped-in user model, because that is what
    the column is a foreign key to."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", self.key[:MAX_ACTOR_LENGTH])
        object.__setattr__(self, "label", self.label[:MAX_ACTOR_LENGTH])

    def __bool__(self) -> bool:
        return bool(self.key or self.label or self.user_pk is not None)


@dataclass(frozen=True, slots=True)
class Scope:
    """The ambient facts captured onto every event fired inside a block.

    Frozen, and nested blocks build a new one rather than mutating: a scope that
    could be edited from inside would let a receiver rewrite the attribution of
    events fired after it.
    """

    actor: Actor = field(default_factory=Actor)
    correlation_id: UUID | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def merged(self, other: Scope) -> Scope:
        """Layer another scope over this one, keeping what it does not set."""
        return replace(
            self,
            actor=other.actor if other.actor else self.actor,
            correlation_id=other.correlation_id or self.correlation_id,
            data={**self.data, **other.data},
        )
