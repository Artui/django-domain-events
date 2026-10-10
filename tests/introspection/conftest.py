"""Fixtures for the introspection queries, which are scraped on a schedule."""

from __future__ import annotations

import re
from collections.abc import Callable

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext

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
