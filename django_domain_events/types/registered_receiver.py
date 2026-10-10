from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_failure import DeliveryFailure
from django_domain_events.types.delivery_mode import DeliveryMode

DEFAULT_LANE = "default"
"""The lane of every receiver that names none, and of every row whose receiver
no longer exists: a relay serving it claims everything no named lane takes."""


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

    targets: Callable[[Any, DeliveryContext], Iterable[str]] | None = None
    """Called at fire time to name this receiver's targets, or None.

    None writes one delivery row per event, as every receiver always has. A
    callable writes one per string it returns, each with its own attempt count,
    backoff and dead-letter, and none when it returns nothing. It runs inside
    the caller's transaction; see ``receiver`` for what that obliges."""

    backoff_base_seconds: float | None = None
    """None means the BACKOFF_BASE_SECONDS setting.

    Read from here when an attempt fails, never copied onto the row, so a
    changed curve reaches deliveries already in flight. ``max_attempts`` is the
    opposite on purpose: it is copied, so lowering it cannot dead-letter them."""

    backoff_cap_seconds: float | None = None
    """None means the BACKOFF_CAP_SECONDS setting. Read like the base."""

    lane: str = DEFAULT_LANE
    """Which relay processes claim this receiver's rows.

    Read from here at claim time, never copied onto the row, so moving a
    receiver to another lane moves the deliveries it is still owed with it."""
