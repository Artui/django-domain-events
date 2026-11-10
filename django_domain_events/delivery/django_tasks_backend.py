from __future__ import annotations

from django_domain_events.delivery.deliver import deliver_one


def deliver_delivery(delivery_id: int, claimed_by: str, claimed_at: str) -> None:
    """Run one delivery."""
    deliver_one(delivery_id, claimed_by=claimed_by, claimed_at=claimed_at)


class DjangoTasksBackend:
    """Hands deliveries to Django's Tasks framework."""

    def __init__(self, queue_name: str = "default") -> None:
        self.queue_name = queue_name

    def enqueue(self, delivery_id: int, claimed_by: str, claimed_at: str) -> None:
        _task()(queue_name=self.queue_name)(deliver_delivery).enqueue(
            delivery_id, claimed_by, claimed_at
        )


def _task():
    """``django.tasks`` in core from 6.0, ``django_tasks`` from the backport."""
    try:
        from django.tasks import task
    except ImportError:
        from django_tasks import task
    return task
