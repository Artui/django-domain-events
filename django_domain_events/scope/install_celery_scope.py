from __future__ import annotations

import json
from contextvars import Token
from typing import Any
from uuid import UUID
from weakref import WeakSet

from django.core.serializers.json import DjangoJSONEncoder

from django_domain_events.scope.attributed import _scope, current_scope
from django_domain_events.scope.causation import inherited_correlation_id
from django_domain_events.types.scope import Actor, Scope

#: Stable, like the Celery backend's task name: messages already on the broker carry it.
HEADER = "django_domain_events_scope"

_installed: WeakSet[Any] = WeakSet()
_tokens: dict[str, Token[Scope | None]] = {}


def install_celery_scope(app: Any) -> None:
    """Carry the publisher's scope into the Celery tasks it starts.

    ``propagate_scope()`` covers threads; a Celery task runs in another process,
    so an event it fires would otherwise record no actor. Call this once where the
    Celery app is built:

        app = Celery("proj")
        install_celery_scope(app)

    Publishing writes the current scope's actor, data and correlation id into one
    message header, as JSON; a task of an installed app runs inside that scope and
    leaves it when it ends, raised or not. Nothing is written when there is no
    scope, and a message without the header runs as before.

    Installing twice is a no-op. The publish signal names no app, so once any app
    is installed the header rides on every message this process publishes; only an
    installed app's tasks read it. ``suppressed()`` and the causing event do not
    travel, only the chain.
    """
    # Celery is the [celery] extra; importing it at module level would make the root require it.
    from celery import signals

    _installed.add(app)
    signals.before_task_publish.connect(_write_header, dispatch_uid=f"{HEADER}.publish")
    signals.task_prerun.connect(_enter, dispatch_uid=f"{HEADER}.prerun")
    signals.task_postrun.connect(_exit, dispatch_uid=f"{HEADER}.postrun")


def _write_header(headers: dict[str, Any], **_: Any) -> None:
    """Stamp the scope in force at publish time; every scope has a chain, so none means none."""
    scope = current_scope()
    correlation_id = scope.correlation_id or inherited_correlation_id()
    if correlation_id is None:
        return
    actor = scope.actor
    headers[HEADER] = json.dumps(
        {
            "actor": {"key": actor.key, "label": actor.label, "user_pk": actor.user_pk},
            "correlation_id": correlation_id,
            "data": scope.data,
        },
        cls=DjangoJSONEncoder,
    )


def _enter(task_id: str, task: Any, **_: Any) -> None:
    """Enter the published scope for an installed app's task that carries one."""
    if task.app not in _installed:
        return
    header = getattr(task.request, HEADER, None)
    if header is None:
        return
    carried = json.loads(header)
    correlation_id = carried["correlation_id"]
    _tokens[task_id] = _scope.set(
        Scope(
            actor=Actor(**carried["actor"]),
            correlation_id=None if correlation_id is None else UUID(correlation_id),
            data=carried["data"],
        )
    )


def _exit(task_id: str, **_: Any) -> None:
    """Leave the scope ``_enter`` set for this task, if it set one."""
    token = _tokens.pop(task_id, None)
    if token is not None:
        _scope.reset(token)
