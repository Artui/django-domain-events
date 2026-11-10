from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

from django_domain_events.scope.causation import inherited_correlation_id
from django_domain_events.types.scope import Actor, Scope

# Default None rather than a Scope instance. A default would be shared by every
# context that never sets one, and Scope carries a dict -- so anything handed
# that dict could edit what every later unattributed event records.
_scope: ContextVar[Scope | None] = ContextVar("django_domain_events_scope", default=None)


@contextmanager
def attributed(
    *,
    actor: Any = None,
    actor_key: str = "",
    actor_label: str = "",
    correlation_id: UUID | None = None,
    **data: Any,
) -> Generator[Scope]:
    """Attach ambient facts to every event fired inside this block."""
    incoming = Scope(
        actor=_actor_from(actor, actor_key, actor_label),
        correlation_id=correlation_id,
        data=_json_safe(data),
    )
    merged = current_scope().merged(incoming)
    if merged.correlation_id is None:
        merged = replace(merged, correlation_id=inherited_correlation_id() or uuid4())

    token = _scope.set(merged)
    try:
        yield merged
    finally:
        _scope.reset(token)


def current_scope() -> Scope:
    """The scope in effect right now."""
    return _scope.get() or Scope()


def _actor_from(actor: Any, actor_key: str, actor_label: str) -> Actor:
    """Derive the actor triple, once, at capture."""
    if getattr(actor, "is_anonymous", False):
        actor = None
    if actor is None and not actor_key and not actor_label:
        return Actor()
    if actor is None:
        return Actor(key=actor_key, label=actor_label or actor_key)
    return Actor(
        key=actor_key or _key_for(actor),
        label=actor_label or str(actor),
        user_pk=_user_pk(actor),
    )


def _key_for(actor: Any) -> str:
    meta = getattr(actor, "_meta", None)
    if meta is None:
        return str(actor)
    return f"{meta.app_label}.{meta.object_name}:{actor.pk}"


def _user_pk(actor: Any) -> Any:
    """The primary key only when the actor really is the user model."""
    from django.contrib.auth import get_user_model

    user_model = get_user_model()
    if not isinstance(actor, user_model) or actor.pk is None:
        return None
    return actor.pk


def _json_safe(data: dict[str, Any]) -> dict[str, Any]:
    """Refuse un-encodable scope data here, where the caller can see why."""
    try:
        json.dumps(data)
    except TypeError as exc:
        raise TypeError(
            f"attributed() data must be JSON-serialisable, and this is not: {exc}"
        ) from exc
    return data
