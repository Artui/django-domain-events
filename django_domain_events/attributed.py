from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

from django_domain_events.causation import inherited_correlation_id
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
) -> Iterator[Scope]:
    """Attach ambient facts to every event fired inside this block.

    Nested blocks layer: an inner one overrides what it sets and inherits the
    rest, so a request-level actor survives a block that only adds a source. The
    actor is one such thing - naming any part of it replaces all of it, because
    a key from one block beside a user id from another describes two actors.

    ``actor`` is any object; its identity is derived once, here. Only an instance
    of the user model reaches the ``actor`` column, which is a foreign key to it;
    everything else is identified by ``actor_key`` alone.
    """
    incoming = Scope(
        actor=_actor_from(actor, actor_key, actor_label),
        correlation_id=correlation_id,
        data=_json_safe(data),
    )
    merged = current_scope().merged(incoming)
    if merged.correlation_id is None:
        # Inherit before minting. Inside a receiver the parent's chain comes off
        # its row, and a block that named itself there would otherwise start a
        # new tree and detach everything it fires from the request that caused it.
        merged = replace(merged, correlation_id=inherited_correlation_id() or uuid4())

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
    """The scope in effect right now. Read at fire time, never later.

    A fresh empty Scope when none is set, not a shared one: a singleton's dict
    would be handed to callers and to receivers, and one mutation would reach
    every later event in the process.
    """
    return _scope.get() or Scope()


def _actor_from(actor: Any, actor_key: str, actor_label: str) -> Actor:
    """Derive the actor triple, once, at capture.

    An unauthenticated request has an actor object that is not a user, and the
    README tells people to pass ``request.user`` - so treating it as absent is
    what keeps "nobody was signed in" from being recorded as somebody called
    AnonymousUser.
    """
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
    """The primary key only when the actor really is the user model.

    Any model has a ``pk``, and writing one into a foreign key that targets the
    user model either names a different person who happens to hold that id, or
    violates the constraint from inside the caller's transaction.
    """
    from django.contrib.auth import get_user_model

    user_model = get_user_model()
    if not isinstance(actor, user_model) or actor.pk is None:
        return None
    return actor.pk


def _json_safe(data: dict[str, Any]) -> dict[str, Any]:
    """Refuse un-encodable scope data here, where the caller can see why.

    ``attributed()`` is meant for middleware, so a value that cannot be written
    would otherwise raise from inside ``fire()`` - arbitrarily far away, and
    inside the business transaction it then takes down with it.
    """
    try:
        json.dumps(data)
    except TypeError as exc:
        raise TypeError(
            f"attributed() data must be JSON-serialisable, and this is not: {exc}"
        ) from exc
    return data
