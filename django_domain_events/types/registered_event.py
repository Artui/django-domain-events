"""What the registry knows about one declared event class."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RegisteredEvent:
    event_class: type
    name: str
    version: int
