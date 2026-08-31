from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from django_domain_events.attributed import attributed, current_scope
from django_domain_events.propagate_scope import propagate_scope
from django_domain_events.suppressed import suppressed, suppression_for
from tests.testapp.events import OrderPlaced


def test_a_bare_thread_loses_the_scope() -> None:
    """Pinned because nothing fails when it happens: events simply arrive with
    no actor. A thread starts with an empty context, not a copy of yours."""
    seen: list[str] = []
    with attributed(actor_key="system:parent"):
        thread = threading.Thread(target=lambda: seen.append(current_scope().actor.key))
        thread.start()
        thread.join()
    assert seen == [""]


def test_it_carries_the_scope_into_a_worker() -> None:
    seen: list[str] = []
    with attributed(actor_key="system:parent"), ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(propagate_scope(lambda: seen.append(current_scope().actor.key))).result()
    assert seen == ["system:parent"]


def test_one_wrapper_can_be_submitted_many_times() -> None:
    """The natural fan-out shape. Capturing a contextvars.Context instead of the
    values would raise on the second submit, because one Context cannot be
    entered twice."""
    seen: list[str] = []
    with attributed(actor_key="system:parent"), ThreadPoolExecutor(max_workers=3) as pool:
        wrapped = propagate_scope(lambda: seen.append(current_scope().actor.key))
        for future in [pool.submit(wrapped) for _ in range(5)]:
            future.result()
    assert seen == ["system:parent"] * 5


def test_it_carries_suppression_too() -> None:
    """A worker doing the same import as its parent should inherit the same
    decision about what not to deliver."""
    seen: list[object] = []
    with suppressed(OrderPlaced, reason="import"), ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(propagate_scope(lambda: seen.append(suppression_for(OrderPlaced)))).result()
    assert seen == [("import", True)]


def test_it_does_not_leak_into_the_next_task_on_a_reused_thread() -> None:
    """Pool threads are reused, so the wrapper has to put the scope back."""
    seen: list[str] = []
    with ThreadPoolExecutor(max_workers=1) as pool:
        with attributed(actor_key="system:first"):
            pool.submit(propagate_scope(lambda: seen.append(current_scope().actor.key))).result()
        pool.submit(lambda: seen.append(current_scope().actor.key)).result()
    assert seen == ["system:first", ""]


def test_the_wrapper_keeps_the_name_of_what_it_wraps() -> None:
    """Read off tracebacks, which is exactly what a thread pool leaves you."""

    def do_the_import() -> None: ...

    assert propagate_scope(do_the_import).__name__ == "do_the_import"
