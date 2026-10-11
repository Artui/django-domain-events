from __future__ import annotations

import pytest
from django.db import connection

from django_domain_events.delivery import wake as wake_module
from django_domain_events.delivery.wake import notify_relay, wait_for_work

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture(autouse=True)
def _no_notification_left_over() -> None:
    """The last-sent times are process state, so a NOTIFY an earlier test (or an
    earlier ``fire()``) sent would suppress the one this test expects to see."""
    wake_module._last_sent.clear()


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

    def __init__(self, notifications: int = 0, alias: str = "default") -> None:
        self.alias = alias
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


class FakeClock:
    """Time that moves only when told to, so no test sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


NOTIFY = 'NOTIFY "django_domain_events"'


def test_polling_sends_no_notification(settings) -> None:
    """``WAKE = "poll"`` is the switch, and the vendor is deliberately Postgres
    here: with ``supported=False`` the vendor gate would answer instead, and
    deleting the setting check would still pass."""
    settings.DJANGO_DOMAIN_EVENTS = {"WAKE": "poll"}
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == []


def test_polling_does_not_listen_either(settings) -> None:
    """A relay under ``poll`` sleeps for the interval, so the same setting
    reads the same on both halves."""
    settings.DJANGO_DOMAIN_EVENTS = {"WAKE": "poll"}
    slept: list[float] = []
    connection = FakeConnection(notifications=1)
    assert wait_for_work(0.5, sleep=slept.append, connection=connection) is False
    assert slept == [0.5]
    assert connection.sql == []


def test_notify_is_the_default_on_postgres() -> None:
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == [NOTIFY]


def test_two_notifications_inside_the_interval_send_one(settings) -> None:
    """Deleting the coalescing sends two. The interval is made large rather than
    the clock injected, so this fails on the pre-change signature too."""
    settings.DJANGO_DOMAIN_EVENTS = {"NOTIFY_COALESCE_SECONDS": 3600}
    connection = FakeConnection()
    notify_relay(connection=connection)
    notify_relay(connection=connection)
    assert connection.sql == [NOTIFY]


def test_a_notification_after_the_interval_sends_another() -> None:
    """Holds the ``now - last < interval`` conjunct: without it nothing is sent
    again once the first has gone."""
    clock = FakeClock()
    connection = FakeConnection()
    notify_relay(connection=connection, clock=clock)
    clock.now += 0.49
    notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY]
    clock.now += 0.02
    notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY, NOTIFY]


def test_a_skipped_notification_does_not_extend_the_interval() -> None:
    """The interval runs from the last NOTIFY actually sent. Restarting it on a
    skipped one would let a steady stream of fires suppress the wake forever."""
    clock = FakeClock()
    connection = FakeConnection()
    notify_relay(connection=connection, clock=clock)
    for _ in range(4):
        clock.now += 0.2
        notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY, NOTIFY]


def test_a_zero_interval_turns_coalescing_off(settings) -> None:
    """Zero sends every notification, even two at the same instant. Holds the
    strict ``<`` in the guard: ``<=`` would read an elapsed time of zero as
    inside a zero-length interval."""
    settings.DJANGO_DOMAIN_EVENTS = {"NOTIFY_COALESCE_SECONDS": 0}
    connection = FakeConnection()
    clock = FakeClock()
    notify_relay(connection=connection, clock=clock)
    notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY, NOTIFY]


def test_each_database_is_coalesced_on_its_own() -> None:
    """A relay listens on one database. A NOTIFY on ``default`` says nothing to a
    relay on ``events``, so it must not suppress the one that does."""
    clock = FakeClock()
    default, events = FakeConnection(alias="default"), FakeConnection(alias="events")
    notify_relay(connection=default, clock=clock)
    notify_relay(connection=events, clock=clock)
    notify_relay(connection=default, clock=clock)
    assert default.sql == [NOTIFY]
    assert events.sql == [NOTIFY]


def test_a_backend_that_cannot_notify_does_not_start_an_interval() -> None:
    """The time is recorded only when a NOTIFY goes out, so the vendor gate
    cannot leave a stale entry that silences the next real one."""
    clock = FakeClock()
    notify_relay(supported=False, connection=FakeConnection(), clock=clock)
    connection = FakeConnection()
    notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY]
