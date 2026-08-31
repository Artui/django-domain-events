from __future__ import annotations

from django_domain_events.deliver import deliver_one


def deliver_delivery(delivery_id: int) -> None:
    """Run one delivery. The task body, kept importable by its own dotted path.

    A task backend has to be able to find this by name in a worker process, so
    it cannot be a closure or a method.
    """
    deliver_one(delivery_id)


class DjangoTasksBackend:
    """Hands deliveries to Django's Tasks framework.

    Django Tasks rather than Celery first: ``django.tasks`` is in core from 6.0
    and the ``django-tasks`` backport covers 4.2 through 6.0, so it spans this
    package's whole supported range with no broker to install.

    The import is lazy because the framework is not a dependency - a consumer
    using the relay directly should not have to have it.
    """

    def __init__(self, queue_name: str = "default") -> None:
        self.queue_name = queue_name

    def enqueue(self, delivery_id: int) -> None:
        _task()(queue_name=self.queue_name)(deliver_delivery).enqueue(delivery_id)


def _task():
    """``django.tasks`` in core from 6.0, ``django_tasks`` from the backport.

    Two import paths for one framework, and the package supports Django 4.2, so
    both are real. Trying core first means a project that has moved on does not
    keep resolving the backport it no longer needs.
    """
    try:
        from django.tasks import task
    except ImportError:
        from django_tasks import task
    return task
