from __future__ import annotations

from collections.abc import Callable
from contextvars import copy_context
from typing import Any, TypeVar

R = TypeVar("R")


def propagate_scope(func: Callable[..., R]) -> Callable[..., R]:
    """Carry the current scope into a thread you start yourself.

    ``threading.Thread`` and ``ThreadPoolExecutor.submit`` start with an *empty*
    context, not a copy of yours - so a worker thread silently loses the
    attribution of whoever spawned it. This is the gotcha that actually bites,
    because nothing fails: events simply arrive with no actor.

    ``executor.submit(propagate_scope(fn), *args)`` runs ``fn`` in a copy of the
    context taken now, at submit time, which is the moment the scope is still
    the caller's.

    Not needed across ``sync_to_async`` / ``async_to_sync``: asgiref copies
    context both ways. Not needed for ``asyncio`` tasks either, which inherit a
    copy at creation. And it cannot help across a process boundary, where the
    answer is the event row rather than a context at all.
    """
    context = copy_context()

    def run(*args: Any, **kwargs: Any) -> R:
        return context.run(func, *args, **kwargs)

    return run
