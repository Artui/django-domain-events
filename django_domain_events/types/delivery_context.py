from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    """Delivery metadata, passed to receivers declaring ``takes_context``.

    Also to a ``targets=`` callable, at fire time, and to the
    ``AROUND_DELIVERY`` hook, once per durable attempt.

    Frozen data read off the event row, never a live handle: a durable delivery
    can run in another process hours after the scope that produced it has gone.
    """

    event_id: int
    event_name: str
    attempt: int
    actor_key: str
    actor_label: str
    scope: dict[str, Any]

    target: str = ""
    """Which target of a fan-out receiver this delivery is for.

    Blank for a receiver declared without ``targets=``, and for any row written
    before the receiver gained one - which is the case worth handling: adding
    ``targets=`` to a receiver with deliveries still owed leaves those rows
    blank, and they are delivered as they were written.

    Last, and defaulted, so adding it does not break a consumer constructing one
    - this is an exported type."""

    delivery_id: int | None = None
    """The primary key of the delivery row this attempt is for.

    Set only on the context the relay or a task builds for a delivery. None
    where no row exists: the context ``targets=`` callables receive at fire
    time, which is what decides the rows, and the one INLINE and ON_COMMIT
    receivers receive, which never have one. Optional and last for the same
    reason as ``target``."""

    due_at: datetime | None = None
    """When this delivery became due.

    For a receiver declared with ``due=``, the due time its row was written
    with - which is what a scheduled receiver compares against: recompute the
    due time from current state, and if it has moved later since the row was
    written (the offer was extended), answer with ``NotYet`` rather than doing
    the work early. For any other row, when it became owed: the moment a
    replay or requeue reopened it, or else when its event was recorded.

    None where no row exists, as for ``delivery_id``. Optional and last for the
    same reason as ``target``."""

    receiver_key: str = ""
    """The key of the receiver this context was built for.

    Set wherever a receiver is known: on every durable delivery, on the context
    an INLINE or ON_COMMIT receiver receives, and on the one a ``targets=``
    callable is asked with. What an ``AROUND_DELIVERY`` hook labels a span, a
    metric or a log line by, since the hook is handed this context and not the
    receiver. Blank only on a context built by hand. Optional and last for the
    same reason as ``target``."""

    occurred_at: datetime | None = None
    """When the event happened, as ``fire(occurred_at=)`` set it.

    What an audit trail records, and what a ``NotYet`` receiver can bound its
    own wait with: stop deferring once the event is older than it allows. None
    only on a context built by hand. Optional and last for the same reason as
    ``target``."""

    correlation_id: UUID | None = None
    """The correlation id the event was recorded with, or None if it had none.

    Optional and last for the same reason as ``target``."""
