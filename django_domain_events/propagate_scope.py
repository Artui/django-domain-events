from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, TypeVar

R = TypeVar("R")


def propagate_scope(func: Callable[..., R]) -> Callable[..., R]:
    """Carry the current scope into a thread you start yourself.

    ``threading.Thread`` and ``ThreadPoolExecutor.submit`` start with an *empty*
    context, not a copy of yours - so a worker thread silently loses the
    attribution of whoever spawned it. This is the gotcha that actually bites,
    because nothing fails: events simply arrive with no actor.

        executor.submit(propagate_scope(fn), *args)

    Call it at submit time, not as a ``@propagate_scope`` decorator. It
    captures the scope when it is called, and at decoration time - import time -
    there is none, so the decorator form silently carries nothing, which is the
    exact failure it exists to prevent.

    It captures the scope's *values* rather than a ``contextvars.Context``, so
    the wrapper is reusable: one Context cannot be entered twice, and a fan-out
    that submits the same wrapped callable per item would raise on the second.

    Not needed across ``sync_to_async`` / ``async_to_sync``, which carry context
    both ways, nor for ``asyncio`` tasks, which inherit a copy at creation. And
    it cannot help across a process boundary, where the answer is the event row.
    """
    from django_domain_events.attributed import _scope, current_scope
    from django_domain_events.causation import _cause
    from django_domain_events.suppressed import _stack

    scope = current_scope()
    cause = _cause.get()
    suppressions = _stack.get()

    @functools.wraps(func)
    def run(*args: Any, **kwargs: Any) -> R:
        tokens = (_scope.set(scope), _cause.set(cause), _stack.set(suppressions))
        try:
            return func(*args, **kwargs)
        finally:
            _scope.reset(tokens[0])
            _cause.reset(tokens[1])
            _stack.reset(tokens[2])

    return run
