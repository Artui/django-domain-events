from __future__ import annotations

import pytest
from django.db import connection

from django_domain_events.wake import notify_relay, wait_for_work

pytestmark = pytest.mark.django_db(transaction=True)


def test_it_sleeps_where_the_backend_cannot_notify() -> None:
    """The relay loop reads the same on every database; only the waiting differs.
    ``supported`` is an argument so both branches are reachable from either
    backend, rather than each one needing the database that has it."""
    slept: list[float] = []
    assert wait_for_work(0.05, supported=False, sleep=slept.append) is False
    assert slept == [0.05]


def test_notifying_is_a_no_op_where_the_backend_cannot() -> None:
    """A SQLite deployment should not have to know this feature exists."""
    assert notify_relay(supported=False) is None


def test_it_times_out_when_nothing_is_owed() -> None:
    """A notification sent while nobody was listening is lost, which is why the
    poll stays as the floor rather than being replaced."""
    if connection.vendor != "postgresql":
        pytest.skip("LISTEN/NOTIFY is Postgres only")
    assert wait_for_work(0.2) is False


def test_a_notification_wakes_the_waiter() -> None:
    if connection.vendor != "postgresql":
        pytest.skip("LISTEN/NOTIFY is Postgres only")
    import threading
    import time

    def notify_soon() -> None:
        time.sleep(0.1)
        from django.db import connections

        connections["default"].close()
        notify_relay()

    thread = threading.Thread(target=notify_soon)
    thread.start()
    try:
        assert wait_for_work(5.0) is True
    finally:
        thread.join()


class FakeCursor:
    """Records the SQL issued, so the statements are asserted rather than only
    executed on the one backend that accepts them."""

    def __init__(self, sink: list[str]) -> None:
        self.sink = sink

    def execute(self, sql: str) -> None:
        self.sink.append(sql)

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class FakeConnection:
    vendor = "postgresql"

    def __init__(self, notifications: int = 0) -> None:
        self.sql: list[str] = []
        self.connection = FakeDriver(notifications)

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.sql)


class FakeDriver:
    def __init__(self, notifications: int) -> None:
        self.notifications = notifications

    def notifies(self, timeout: float, stop_after: int):
        for _ in range(self.notifications):
            yield object()


def test_it_notifies_on_the_package_channel() -> None:
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == ['NOTIFY "django_domain_events"']


def test_waiting_listens_before_it_waits() -> None:
    """LISTEN has to be issued on the connection that then waits, and before it
    waits: a notification sent in between is lost, which is the whole reason the
    poll remains the floor."""
    connection = FakeConnection(notifications=1)
    assert wait_for_work(1.0, connection=connection) is True
    assert connection.sql == ['LISTEN "django_domain_events"']


def test_waiting_reports_a_timeout() -> None:
    assert wait_for_work(1.0, connection=FakeConnection(notifications=0)) is False
