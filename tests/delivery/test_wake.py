from __future__ import annotations

import time

import pytest
from django.db import connection
from django.db.backends.postgresql import psycopg_any

from django_domain_events.delivery import wake as wake_module
from django_domain_events.delivery.wake import notify_relay, wait_for_work

pytestmark = pytest.mark.django_db(transaction=True)

# The driver Django loaded, read before any test stands a different one in.
# Every environment the suite runs in has one: psycopg 3 from the dev group, or
# psycopg2 in the CI job that swaps it in.
LOADED_IS_PSYCOPG3 = psycopg_any.is_psycopg3
PSYCOPG2_CANNOT_LISTEN = "psycopg2 has no blocking notifies(); the relay polls there"


@pytest.fixture(autouse=True)
def _no_notification_left_over() -> None:
    """The last-sent times are process state, so a NOTIFY an earlier test (or an
    earlier ``fire()``) sent would suppress the one this test expects to see."""
    wake_module._last_sent.clear()


@pytest.fixture(autouse=True)
def _fakes_stand_for_psycopg3(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake connections below model psycopg 3's ``notifies``, so they run as
    psycopg 3 whichever driver is installed, and the coalescing and draining
    they hold are asserted under psycopg2 too. A test about the driver itself
    sets it again, or reads ``LOADED_IS_PSYCOPG3``, which this does not touch."""
    monkeypatch.setattr(psycopg_any, "is_psycopg3", True)


def test_it_sleeps_where_the_backend_cannot_notify() -> None:
    """The relay loop reads the same on every database; only the waiting differs."""
    slept: list[float] = []
    assert wait_for_work(0.05, supported=False, sleep=slept.append) is False
    assert slept == [0.05]


def test_notifying_is_a_no_op_where_the_backend_cannot() -> None:
    """A SQLite deployment should not have to know this feature exists."""
    assert notify_relay(supported=False) is None


def test_it_times_out_when_nothing_is_owed() -> None:
    """A notification sent while nobody listens is lost, so the poll stays as the floor."""
    if connection.vendor != "postgresql":
        pytest.skip("LISTEN/NOTIFY is Postgres only")
    if not LOADED_IS_PSYCOPG3:
        pytest.skip(PSYCOPG2_CANNOT_LISTEN)
    assert wait_for_work(0.2) is False


def test_a_notification_wakes_the_waiter() -> None:
    if connection.vendor != "postgresql":
        pytest.skip("LISTEN/NOTIFY is Postgres only")
    if not LOADED_IS_PSYCOPG3:
        pytest.skip(PSYCOPG2_CANNOT_LISTEN)
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


def test_notifications_queued_while_busy_wake_the_relay_once() -> None:
    """A busy relay queues notifications on its connection for the next wait."""
    if connection.vendor != "postgresql":
        pytest.skip("LISTEN/NOTIFY is Postgres only")
    if not LOADED_IS_PSYCOPG3:
        pytest.skip(PSYCOPG2_CANNOT_LISTEN)
    import psycopg
    from django.db import connections

    listener = connections["default"]
    assert wait_for_work(0.01) is False
    params = listener.get_connection_params()
    with psycopg.connect(**params, autocommit=True) as sender:
        for i in range(5):
            sender.execute("SELECT pg_notify(%s, %s)", (wake_module.CHANNEL, str(i)))
    time.sleep(0.1)
    with listener.cursor() as cursor:
        cursor.execute("SELECT 1")

    assert wait_for_work(5.0) is True
    started = time.monotonic()
    assert wait_for_work(0.3) is False
    assert time.monotonic() - started >= 0.25


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

    def __init__(
        self, notifications: int = 0, alias: str = "default", arriving: tuple[int, ...] = ()
    ) -> None:
        self.alias = alias
        self.sql: list[str] = []
        self.connection = FakeDriver(notifications, arriving=arriving)

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.sql)


class FakeDriver:
    """psycopg's notifies (3.2.4 onwards), down to the parts the wait depends on."""

    def __init__(self, notifications: int, *, arriving: tuple[int, ...] = ()) -> None:
        self.notifications = notifications
        self.arriving = list(arriving)

    def notifies(self, *, timeout: float, stop_after: int | None = None):
        received = 0
        while True:
            if self.notifications:
                while self.notifications:
                    self.notifications -= 1
                    received += 1
                    yield object()
            elif self.arriving:
                for _ in range(self.arriving.pop(0)):
                    received += 1
                    yield object()
            else:
                return
            if (stop_after is not None and received >= stop_after) or timeout == 0:
                return


def test_it_notifies_on_the_package_channel() -> None:
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == ['NOTIFY "django_domain_events"']


def test_waiting_listens_before_it_waits() -> None:
    """LISTEN is issued on the waiting connection, before the wait, or it is lost."""
    connection = FakeConnection(notifications=1)
    assert wait_for_work(1.0, connection=connection) is True
    assert connection.sql == ['LISTEN "django_domain_events"']


def test_one_wake_drains_what_queued_behind_it() -> None:
    """The backlog half of the Postgres test: three queued, one wake, then nothing."""
    connection = FakeConnection(notifications=3)
    assert wait_for_work(1.0, connection=connection) is True
    assert connection.connection.notifications == 0
    assert wait_for_work(1.0, connection=connection) is False


def test_one_wake_also_takes_what_arrived_after_its_wait_returned() -> None:
    """The backlog empties on its own, since psycopg yields all of it before stop_after."""
    connection = FakeConnection(notifications=2, arriving=(1,))
    assert wait_for_work(1.0, connection=connection) is True
    assert connection.connection.arriving == []
    assert wait_for_work(1.0, connection=connection) is False


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
    """WAKE = 'poll' is the switch, not the vendor gate, so both are tested."""
    settings.DJANGO_DOMAIN_EVENTS = {"WAKE": "poll"}
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == []


def test_polling_does_not_listen_either(settings) -> None:
    """A relay under poll sleeps for the interval."""
    settings.DJANGO_DOMAIN_EVENTS = {"WAKE": "poll"}
    slept: list[float] = []
    connection = FakeConnection(notifications=1)
    assert wait_for_work(0.5, sleep=slept.append, connection=connection) is False
    assert slept == [0.5]
    assert connection.sql == []


def test_a_driver_that_cannot_listen_polls(monkeypatch: pytest.MonkeyPatch) -> None:
    """psycopg2 fills notifies on poll(), so the relay sleeps and issues no LISTEN."""
    monkeypatch.setattr(psycopg_any, "is_psycopg3", False)
    slept: list[float] = []
    connection = FakeConnection(notifications=1)
    assert wait_for_work(0.5, sleep=slept.append, connection=connection) is False
    assert slept == [0.5]
    assert connection.sql == []


def test_a_driver_that_cannot_listen_sends_no_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both halves read one answer: a NOTIFY nothing hears buys nothing."""
    monkeypatch.setattr(psycopg_any, "is_psycopg3", False)
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == []


def test_psycopg2_polls_on_a_real_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same on the server, through the driver Django actually loaded."""
    if connection.vendor != "postgresql" or LOADED_IS_PSYCOPG3:
        pytest.skip("needs Postgres under psycopg2")
    monkeypatch.setattr(psycopg_any, "is_psycopg3", LOADED_IS_PSYCOPG3)
    slept: list[float] = []
    assert wait_for_work(0.05, sleep=slept.append) is False
    assert slept == [0.05]


def test_notify_is_the_default_on_postgres() -> None:
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == [NOTIFY]


def test_two_notifications_inside_the_interval_send_one(settings) -> None:
    """Deleting the coalescing sends two."""
    settings.DJANGO_DOMAIN_EVENTS = {"NOTIFY_COALESCE_SECONDS": 3600}
    connection = FakeConnection()
    notify_relay(connection=connection)
    notify_relay(connection=connection)
    assert connection.sql == [NOTIFY]


def test_a_notification_after_the_interval_sends_another() -> None:
    """Holds the 'now - last < interval' conjunct: without it nothing re-sends."""
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
    """The interval runs from the last NOTIFY actually sent."""
    clock = FakeClock()
    connection = FakeConnection()
    notify_relay(connection=connection, clock=clock)
    for _ in range(4):
        clock.now += 0.2
        notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY, NOTIFY]


def test_a_zero_interval_turns_coalescing_off(settings) -> None:
    """Zero sends every notification, even two at the same instant."""
    settings.DJANGO_DOMAIN_EVENTS = {"NOTIFY_COALESCE_SECONDS": 0}
    connection = FakeConnection()
    clock = FakeClock()
    notify_relay(connection=connection, clock=clock)
    notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY, NOTIFY]


def test_each_database_is_coalesced_on_its_own() -> None:
    """A relay listens on one database."""
    clock = FakeClock()
    default, events = FakeConnection(alias="default"), FakeConnection(alias="events")
    notify_relay(connection=default, clock=clock)
    notify_relay(connection=events, clock=clock)
    notify_relay(connection=default, clock=clock)
    assert default.sql == [NOTIFY]
    assert events.sql == [NOTIFY]


def test_a_backend_that_cannot_notify_does_not_start_an_interval() -> None:
    """The time is recorded only when a NOTIFY goes out, so no stale entry lingers."""
    clock = FakeClock()
    notify_relay(supported=False, connection=FakeConnection(), clock=clock)
    connection = FakeConnection()
    notify_relay(connection=connection, clock=clock)
    assert connection.sql == [NOTIFY]
