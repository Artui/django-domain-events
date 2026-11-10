from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, TypeVar

R = TypeVar("R")


def propagate_scope(func: Callable[..., R]) -> Callable[..., R]:
    """Carry the current scope into a thread you start yourself."""
    from django_domain_events.scope.attributed import _scope, current_scope
    from django_domain_events.scope.causation import _cause
    from django_domain_events.scope.suppressed import _stack

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
