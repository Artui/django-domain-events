from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class Actor:
    """Who acted, as one fact rather than three columns that can disagree."""

    key: str = ""
    label: str = ""
    user_pk: Any = None
    """Only ever a primary key of the swapped-in user model, because that is what
    the column is a foreign key to."""

    def __post_init__(self) -> None:
        from django_domain_events.models.event_record import EventRecord  # AppRegistryNotReady

        limit = EventRecord.actor_key.field.max_length
        assert limit is not None
        if len(self.key) > limit:
            raise ValueError(
                f"Actor was given an actor_key of {len(self.key)} characters; the limit is "
                f"{limit}, the width of the indexed column the event row stores it in. "
                f"Clipping it would merge two actors sharing a prefix into one, and a longer "
                f"one is rejected by the database on Postgres and stored whole on SQLite. "
                f"Shorten the actor_key."
            )
        label_limit = EventRecord.actor_label.field.max_length
        object.__setattr__(self, "label", self.label[:label_limit])

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
