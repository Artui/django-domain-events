from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from django_domain_events.types.delivery_failure import DeliveryFailure
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
    on_failure: Callable[[DeliveryFailure], None] | None = None
    """Called after a failed attempt has been recorded, or None.

    The hook exists because a receiver cannot otherwise keep anything about its
    own failure: it runs inside the transaction that carries its acknowledgement,
    so everything it wrote is rolled back before the failure is recorded. A
    receiver that wants a durable log of what went wrong had no way to write one.

    It runs **outside** that transaction, after the delivery row is updated, so
    what it writes survives. It must not raise: a hook that does is logged and
    swallowed, because the alternative is a failure path that fails."""

    lease_seconds: int | None = None
    """None means the LEASE_SECONDS setting. Defaulted because it is the one
    field of a declaration that is genuinely optional: every other value here
    is something the decorator always resolves."""
