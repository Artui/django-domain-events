"""A hook around each delivery: the delivery and receiver ids on every log line.

Named in settings as ``AROUND_DELIVERY``. The package enters it around every
durable attempt, outside the transaction carrying the acknowledgement, and
exits it once the outcome is recorded. So the lines a receiver logs, the
warning a failed attempt writes, and anything its ``on_failure`` logs all carry
the ids, and no receiver has to set them itself.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from django_domain_events import DeliveryContext

current_delivery: ContextVar[str] = ContextVar("current_delivery", default="-")


@contextmanager
def delivery_log_context(ctx: DeliveryContext) -> Iterator[None]:
    """Set the ids for the length of one attempt, and put back what was there.

    ``receiver_key`` is on the context for exactly this: the hook is handed the
    context, not the receiver. Reset rather than cleared, so whatever was set
    before the attempt is back after it.
    """
    token = current_delivery.set(f"delivery={ctx.delivery_id} receiver={ctx.receiver_key}")
    try:
        yield
    finally:
        current_delivery.reset(token)


@contextmanager
def tracer_that_is_down(ctx: DeliveryContext) -> Iterator[None]:
    """A hook that cannot be entered, as a tracer whose collector is gone.

    The demo switches to it for one order, to show what entering a hook that
    raises does: the attempt fails as if the receiver had raised, and the
    receiver does not run. A hook setting a tenant must not let a receiver run
    without one; an instrumentation hook should guard its own exporter calls.
    """
    raise ConnectionError("the tracing collector is unreachable")
    yield


class DeliveryLogFilter(logging.Filter):
    """Stamp each record with the delivery it was logged inside, or ``-``."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.delivery = current_delivery.get()
        return True
