from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from celery import Celery, signals
from celery.worker.request import Request
from django.contrib.auth import get_user_model
from django.db import transaction

from django_domain_events.delivery.fire import fire
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord
from django_domain_events.scope.attributed import attributed, current_scope
from django_domain_events.scope.causation import caused_by
from django_domain_events.scope.install_celery_scope import HEADER, install_celery_scope
from django_domain_events.scope.suppressed import suppressed
from tests.testapp.events import Currency, OrderPlaced

pytestmark = pytest.mark.django_db

QUEUE = "dde-install-celery-scope"
UIDS = (
    (signals.before_task_publish, "publish"),
    (signals.task_prerun, "prerun"),
    (signals.task_postrun, "postrun"),
)
CHAIN = UUID("5b0c3f8e-2f7a-4f4e-9d61-3c1f0e8a7b21")


def _app(name: str) -> Celery:
    app = Celery(name, set_as_current=False, broker="memory://")
    app.conf.update(task_default_queue=QUEUE)

    @app.task(name=f"{name}.place_order")
    def place_order(order_id: int, fail: bool = False) -> None:
        with transaction.atomic():
            fire(
                OrderPlaced(
                    order_id=order_id,
                    total=Decimal("1.00"),
                    placed_at=datetime(2026, 11, 13, tzinfo=timezone.utc),
                    trace=CHAIN,
                    currency=Currency.EUR,
                    kind="retail",
                    tags=[],
                )
            )
        if fail:
            raise RuntimeError("the task failed after firing")

    return app


@pytest.fixture
def app(caplog: pytest.LogCaptureFixture) -> Iterator[Celery]:
    """A non-eager app on the memory broker; eager mode never sends ``before_task_publish``."""
    yield _app("scoped")
    for signal, stage in UIDS:
        signal.disconnect(dispatch_uid=f"{HEADER}.{stage}")
    swallowed = [
        r for r in caplog.get_records("call") if r.name.startswith("celery.utils.dispatch")
    ]
    assert not swallowed, "Celery logs and swallows a handler's exception"


def _publish(app: Celery, order_id: int, *, fail: bool = False) -> None:
    app.tasks[f"{app.main}.place_order"].apply_async((order_id,), {"fail": fail})


def _run_next(app: Celery) -> dict:
    """Take one message off the broker and run it as a worker would, after the publisher's block."""
    with app.connection_for_read() as conn:
        message = conn.SimpleQueue(QUEUE).get(timeout=1)
    task = app.tasks[message.headers["task"]]
    Request(message, app=app, task=task).execute()
    return message.headers


def _row(order_id: int) -> EventRecord:
    return EventRecord.objects.get(payload__order_id=order_id)


def test_a_task_published_in_scope_records_its_actor_data_and_chain(app: Celery) -> None:
    install_celery_scope(app)
    user = get_user_model().objects.create(username="u")
    with attributed(actor=user, club_id=1) as scope:
        _publish(app, 1)
    _run_next(app)

    row = _row(1)
    assert row.actor == user
    assert (row.actor_key, row.actor_label) == (scope.actor.key, scope.actor.label)
    assert row.scope == {"club_id": 1}
    assert row.correlation_id == scope.correlation_id is not None


def test_a_task_published_outside_any_scope_fires_with_no_actor(app: Celery) -> None:
    """Run after an attributed task on the same thread, so a scope left entered would show."""
    install_celery_scope(app)
    with attributed(actor_key="system:first"):
        _publish(app, 1)
    _publish(app, 2)
    _run_next(app)
    _run_next(app)

    assert _row(1).actor_key == "system:first"
    row = _row(2)
    assert (row.actor, row.actor_key, row.scope) == (None, "", {})


def test_without_install_nothing_changes(app: Celery) -> None:
    with attributed(actor_key="system:parent", club_id=1):
        _publish(app, 1)
    _run_next(app)

    row = _row(1)
    assert (row.actor_key, row.scope) == ("", {})


def test_no_scope_writes_no_header(app: Celery) -> None:
    install_celery_scope(app)
    _publish(app, 1)
    with app.connection_for_read() as conn:
        message = conn.SimpleQueue(QUEUE).get(timeout=1)
    assert HEADER not in message.headers


def test_a_receivers_chain_reaches_the_task_without_attributed(app: Celery) -> None:
    install_celery_scope(app)
    with caused_by(41, CHAIN):
        _publish(app, 1)
    _run_next(app)

    row = _row(1)
    assert (row.correlation_id, row.causation_id) == (CHAIN, None)


def test_suppression_does_not_travel(app: Celery) -> None:
    install_celery_scope(app)
    with suppressed(OrderPlaced, reason="import"):
        _publish(app, 1)
    _run_next(app)

    row = _row(1)
    assert row.suppressed_reason == ""
    assert DeliveryRecord.objects.filter(event=row).exists()


def test_installing_twice_connects_once(app: Celery) -> None:
    install_celery_scope(app)
    install_celery_scope(app)

    with attributed(actor_key="system:parent"):
        _publish(app, 1)
    _run_next(app)
    assert _row(1).actor_key == "system:parent"
    assert current_scope().actor.key == ""


def test_another_apps_tasks_are_left_alone(app: Celery) -> None:
    install_celery_scope(app)
    other = _app("other")
    with attributed(actor_key="system:parent"):
        _publish(other, 1)
    headers = _run_next(other)

    assert HEADER in headers, "the publish signal names no app"
    assert _row(1).actor_key == ""


def test_an_eager_task_keeps_the_callers_scope(app: Celery) -> None:
    """Eager mode publishes nothing, so the task runs inline with no header to read."""
    install_celery_scope(app)
    app.conf.task_always_eager = True
    with attributed(actor_key="system:parent"):
        _publish(app, 1)

    assert _row(1).actor_key == "system:parent"


def test_a_task_that_raises_still_exits_the_scope(app: Celery) -> None:
    install_celery_scope(app)
    with attributed(actor_key="system:parent"):
        _publish(app, 1, fail=True)
    _publish(app, 2)
    _run_next(app)
    assert current_scope().actor.key == ""
    _run_next(app)

    assert _row(1).actor_key == "system:parent"
    assert _row(2).actor_key == ""


def test_a_finished_task_leaves_no_token_behind(app: Celery) -> None:
    """A long-running worker would otherwise hold one token per task it ever ran."""
    install_celery_scope(app)
    with attributed(actor_key="system:parent"):
        _publish(app, 1, fail=True)
        _publish(app, 2)
    _run_next(app)
    _run_next(app)

    assert sys.modules["django_domain_events.scope.install_celery_scope"]._tokens == {}


def test_the_package_root_does_not_import_celery() -> None:
    """The export sits at the root, and the [celery] extra must stay optional."""
    code = "import sys, django_domain_events; print('celery' in sys.modules)"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.stdout.strip() == "False", result.stderr
