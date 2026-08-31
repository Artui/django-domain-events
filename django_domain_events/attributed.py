from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

from django_domain_events.types.scope import Scope

# Default None rather than an empty Scope: a default instance is shared by every
# context that never sets one, and Scope carries a dict. It is frozen and nothing
# mutates it today, which is exactly the kind of guarantee that quietly stops
# being true.
_scope: ContextVar[Scope | None] = ContextVar("django_domain_events_scope", default=None)

EMPTY = Scope()


@contextmanager
def attributed(
    *,
    actor: Any = None,
    actor_key: str = "",
    actor_label: str = "",
    correlation_id: UUID | None = None,
    **data: Any,
) -> Iterator[Scope]:
    """Attach ambient facts to every event fired inside this block.

    Nested blocks layer: an inner one overrides what it sets and inherits the
    rest, so a request-level actor survives a block that only adds a source.

    ``actor`` is any object with a primary key. Its identity is derived once,
    here, into the columns the event row carries - a log that has to join to say
    who acted is a log that loses the answer when the row is deleted.
    """
    incoming = Scope(
        actor_key=actor_key or _key_for(actor),
        actor_label=actor_label or _label_for(actor),
        actor_pk=getattr(actor, "pk", None),
        correlation_id=correlation_id,
        data=data,
    )
    merged = current_scope().merged(incoming)
    if merged.correlation_id is None:
        # The outermost block roots the chain, so every event descended from one
        # request shares an id without anyone threading it through.
        merged = replace(merged, correlation_id=uuid4())

    token = _scope.set(merged)
    try:
        yield merged
    finally:
        # Reset by token rather than by restoring the old value: workers and
        # threads are reused, and a scope left behind bleeds one request's
        # attribution into the next. For an attribution feature that is not
        # untidiness, it is a correctness bug with a privacy flavour.
        _scope.reset(token)


def current_scope() -> Scope:
    """The scope in effect right now. Read at fire time, never later."""
    return _scope.get() or EMPTY


def _key_for(actor: Any) -> str:
    """A stable identity string for any actor, user or not."""
    if actor is None:
        return ""
    meta = getattr(actor, "_meta", None)
    if meta is None:
        return str(actor)
    return f"{meta.app_label}.{meta.object_name}:{actor.pk}"


def _label_for(actor: Any) -> str:
    return "" if actor is None else str(actor)
