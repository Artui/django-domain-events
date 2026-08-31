"""What the registry knows about one declared receiver."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from django_domain_events.types.delivery_mode import DeliveryMode


@dataclass(frozen=True, slots=True)
class RegisteredReceiver:
    """A callable, the event it listens for, and what it promises."""

    key: str
    """Stable identity written onto every delivery row addressed to it.

    Defaults to ``<app_label>.<function_name>`` rather than the dotted import
    path, so moving the module does not orphan pending rows.
    """

    event_class: type
    func: Callable[..., Any]
    mode: DeliveryMode
    takes_context: bool
    max_attempts: int
