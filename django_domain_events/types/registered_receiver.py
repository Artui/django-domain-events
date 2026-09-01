from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from django_domain_events.types.delivery_mode import DeliveryMode


@dataclass(frozen=True, slots=True)
class RegisteredReceiver:
    key: str
    event_class: type
    func: Callable[..., Any]
    mode: DeliveryMode
    takes_context: bool
    max_attempts: int
    eager: bool
    site: str
    lease_seconds: int | None = None
    """None means the LEASE_SECONDS setting. Defaulted because it is the one
    field of a declaration that is genuinely optional: every other value here
    is something the decorator always resolves."""
