"""Shared fixtures."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext

from django_domain_events.declaration.registry import registry
from django_domain_events.types.registered_event import RegisteredEvent
from django_domain_events.types.registered_receiver import RegisteredReceiver
from tests.testapp.events import Currency, OrderPlaced, calls


@pytest.fixture
def record() -> Iterator[list[str]]:
    """The list receivers append to, emptied around each test."""
    calls.clear()
    yield calls
    calls.clear()


@pytest.fixture
def order() -> OrderPlaced:
    """One fully populated event, so a round trip covers every scalar."""
    return OrderPlaced(
        order_id=7,
        total=Decimal("19.99"),
        placed_at=datetime(2026, 8, 31, 9, 0, tzinfo=timezone.utc),
        trace=UUID("d29eb6e4-a54c-4f06-8e3b-f1c416264d37"),
        currency=Currency.EUR,
        kind="retail",
        tags=["priority", "gift"],
    )


@contextmanager
def receiver_deleted(key: str) -> Iterator[None]:
    """Take a receiver out of the live registry, and put it back where it was.

    The whole dict is restored, not just the entry. Re-inserting a popped key
    appends it, and receivers are read in insertion order - so a pop-and-restore
    silently reorders the fan-out for every test that runs afterwards, which
    only shows up as a different delivery order in the full suite.
    """
    original = dict(registry._receivers)
    del registry._receivers[key]
    try:
        yield
    finally:
        registry._receivers.clear()
        registry._receivers.update(original)


@contextmanager
def receiver_replaced(key: str, func: object) -> Iterator[None]:
    """Swap one receiver's callable for the duration of a test."""
    entry = registry.receiver_for_key(key)
    original = entry.func
    object.__setattr__(entry, "func", func)
    try:
        yield
    finally:
        object.__setattr__(entry, "func", original)


@contextmanager
def event_deleted(name: str) -> Iterator[None]:
    """Take one event out of the live registry, and put it back.

    Both indexes, because the registry keeps two and a check reading one while
    a test cleared the other would pass on a registry no consumer can produce.
    """
    entry = registry.event_for_name(name)
    assert entry is not None, f"{name} is not registered"
    del registry._events_by_name[name]
    del registry._events_by_class[entry.event_class]
    try:
        yield
    finally:
        registry.register_event(entry)


@contextmanager
def event_registered(event_class: type, name: str, version: int = 1) -> Iterator[None]:
    """Add an ad-hoc event for the duration of a test."""
    registry.register_event(RegisteredEvent(event_class=event_class, name=name, version=version))
    try:
        yield
    finally:
        registry._events_by_name.pop(name, None)
        registry._events_by_class.pop(event_class, None)


@contextmanager
def receiver_registered(entry: RegisteredReceiver) -> Iterator[None]:
    """Add an ad-hoc receiver for the duration of a test."""
    registry.register_receiver(entry)
    try:
        yield
    finally:
        registry._receivers.pop(entry.key, None)


Plans = Callable[[Callable[[], object]], list[str]]

DELIVERY_TABLE = "django_domain_events_deliveryrecord"


def delivery_table_access(plan: str) -> set[str]:
    """How a plan reads the delivery table: the index names it scans, plus
    ``"Seq Scan"`` when it reads the heap directly.

    Index names alone are not enough. With sequential scans priced out, a
    predicate no partial index matches is answered by walking a *whole*
    index - the unique one, say - and filtering every entry, which never
    reads ``Seq Scan`` and costs exactly as much. So the assertion worth
    making is that every access is through a partial index.
    """
    access = set(re.findall(rf"using (\S+) on {DELIVERY_TABLE}\b", plan))
    access |= {
        name
        for name in re.findall(r"Bitmap Index Scan on (\S+)", plan)
        if not name.startswith("django_domain_events_eventrecord")
    }
    if re.search(rf"Seq Scan on {DELIVERY_TABLE}\b", plan):
        access.add("Seq Scan")
    return access


@pytest.fixture
def plans_without_seqscan() -> Plans:
    """Run a callable and return the Postgres plan of every SELECT it issued.

    With sequential scans priced out, so a plan names an index whenever one
    *can* serve the predicate. On a test-sized table the planner prefers a
    sequential scan regardless, which would make an assertion about index use
    pass or fail on table size rather than on whether the predicate matches
    the index; with them off, a plan still reads ``Seq Scan`` only when no
    index can answer the query at all, and that is the defect being guarded.

    The queries are captured from the public function rather than rebuilt in
    the test, so the plan is of the SQL the function actually sends.
    """

    def run(call: Callable[[], object]) -> list[str]:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL enable_seqscan = off")
            with CaptureQueriesContext(connection) as captured:
                call()
            explained = []
            for query in captured.captured_queries:
                if not query["sql"].startswith("SELECT"):
                    continue
                with connection.cursor() as cursor:
                    cursor.execute(f"EXPLAIN {query['sql']}")
                    explained.append("\n".join(row[0] for row in cursor.fetchall()))
        return explained

    return run
