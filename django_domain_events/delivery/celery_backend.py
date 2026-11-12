from __future__ import annotations

from celery import shared_task

from django_domain_events.delivery.deliver import deliver_one

#: Stable rather than derived from the module path, because messages already on
#: the broker name it: a module moved in a later release would otherwise strand
#: every message enqueued before the upgrade as an unregistered task.
TASK_NAME = "django_domain_events.deliver_delivery"


@shared_task(name=TASK_NAME, ignore_result=True)
def deliver_delivery_task(delivery_id: int, claimed_by: str, claimed_at: str) -> None:
    """Run one delivery in a Celery worker, taking the row under its claim."""
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
        # No queue means Celery routes by task name, so task_routes keeps working unnamed.
        options = {} if self.queue is None else {"queue": self.queue}
        deliver_delivery_task.apply_async(args=(delivery_id, claimed_by, claimed_at), **options)
