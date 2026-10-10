"""A ``TaskBackend`` for Celery, installed with the ``celery`` extra.

Celery is imported at the top of this module rather than inside a function,
which is what the structural rules ask for anyway: this module *is* the opt-in.
Nothing in the package imports it, and ``__init__`` does not re-export it, so it
is loaded only when ``TASK_BACKEND`` names it in the relay or a Celery worker's
``imports`` names it - exactly where Celery is installed. ``dacite_codec`` is
the same arrangement for the same reason.

A function-local import was the obvious alternative and it does not work here.
A Celery worker runs a message by looking its task name up in its own registry,
so the task has to be registered by importing a module in the worker, not by
building it in the relay at the moment of enqueueing. A task built lazily
inside ``enqueue`` passes every eager test and is "unregistered" in every real
worker.
"""

from __future__ import annotations

from celery import shared_task

from django_domain_events.delivery.deliver import deliver_one

#: Stable rather than derived from the module path, because messages already on
#: the broker name it: a module moved in a later release would otherwise strand
#: every message enqueued before the upgrade as an unregistered task.
TASK_NAME = "django_domain_events.deliver_delivery"


@shared_task(name=TASK_NAME, ignore_result=True)
def deliver_delivery_task(delivery_id: int, claimed_by: str, claimed_at: str) -> None:
    """Run one delivery in a Celery worker, taking the row under its claim.

    ``shared_task`` rather than a particular app's ``task``, so it binds to
    whichever app the project configured without this package naming it.
    ``ignore_result`` because the outcome is recorded on the delivery row; a
    result backend would only store a second copy of ``None``.
    """
    deliver_one(delivery_id, claimed_by=claimed_by, claimed_at=claimed_at)


class CeleryBackend:
    """Hands deliveries to Celery.

    For a project whose workers already run Celery. A project without one is
    better served by ``DjangoTasksBackend``, which needs no broker.

    Redelivery is safe: ``acks_late`` and a broker's visibility timeout both
    hand a worker a message whose task already ran, and the claim the message
    carries is what turns that copy into a no-op. Recovery from a worker that
    died mid-delivery comes from the row's lease, not from the redelivery: the
    second copy finds the row taken by the worker that died and leaves it for
    the relay to reclaim.
    """

    def __init__(self, queue: str | None = None) -> None:
        self.queue = queue

    def enqueue(self, delivery_id: int, claimed_by: str, claimed_at: str) -> None:
        # No queue means Celery's own routing decides, so a project routing by
        # task name in ``task_routes`` keeps working without naming it twice.
        options = {} if self.queue is None else {"queue": self.queue}
        deliver_delivery_task.apply_async(args=(delivery_id, claimed_by, claimed_at), **options)
