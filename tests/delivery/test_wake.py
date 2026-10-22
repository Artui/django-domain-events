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
    """A relay busy delivering is not waiting, so the notifications sent meanwhile
    queue on its connection: psycopg keeps those that arrive during other
    statements in a backlog and yields them first on the next wait. Leaving the
    generator after one restores the rest, so without a drain each queued
    notification is one more instant wake - one empty claim transaction apiece -
    before the relay waits for real again."""
    if connection.vendor != "postgresql":
        pytest.skip("LISTEN/NOTIFY is Postgres only")
    if not LOADED_IS_PSYCOPG3:
        pytest.skip(PSYCOPG2_CANNOT_LISTEN)
    import psycopg
    from django.db import connections

    listener = connections["default"]
    # Listening first, as a relay is from its first wait on.
    assert wait_for_work(0.01) is False
    params = listener.get_connection_params()
    with psycopg.connect(**params, autocommit=True) as sender:
        # Distinct payloads in separate statements: Postgres folds identical
        # notifications sent in one transaction into one.
        for i in range(5):
            sender.execute("SELECT pg_notify(%s, %s)", (wake_module.CHANNEL, str(i)))
    # A statement on the listener while they arrive, as a delivery would run,
    # which is what lands them in the backlog rather than on the socket.
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
    """psycopg's ``notifies`` (3.2.4 onwards), down to the parts the wait
    depends on, in the order psycopg checks them.

    ``notifications`` is the backlog the connection collected while it ran
    other statements. A call yields all of it before it looks at
    ``stop_after``, so a generator run to its end empties it. ``arriving`` is
    the socket: each read takes the next batch, standing for what had arrived
    by then, and ``stop_after`` is checked after each batch. A batch still
    waiting after a call returns is one that arrived once it had stopped
    reading, and ``timeout=0`` reads at most once.
    """

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
    """LISTEN has to be issued on the connection that then waits, and before it
    waits: a notification sent in between is lost, which is the whole reason the
    poll remains the floor."""
    connection = FakeConnection(notifications=1)
    assert wait_for_work(1.0, connection=connection) is True
    assert connection.sql == ['LISTEN "django_domain_events"']


def test_one_wake_drains_what_queued_behind_it() -> None:
    """The backlog half of the Postgres test above, on any backend: three
    queued, one wake, and nothing left for the next wait to return at once."""
    connection = FakeConnection(notifications=3)
    assert wait_for_work(1.0, connection=connection) is True
    assert connection.connection.notifications == 0
    assert wait_for_work(1.0, connection=connection) is False


def test_one_wake_also_takes_what_arrived_after_its_wait_returned() -> None:
    """The backlog empties on its own, because psycopg yields all of it before
    it looks at ``stop_after``. What the second, zero-timeout call is for is a
    notification that reached the socket after the first call stopped reading:
    left there, the next wait would return at once for a claim with nothing
    new to find."""
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


def test_a_driver_that_cannot_listen_polls(monkeypatch: pytest.MonkeyPatch) -> None:
    """psycopg2's ``notifies`` is a list the driver fills on ``poll()``, not
    psycopg 3's blocking generator, so the wait cannot run on it: the relay
    sleeps for the poll and issues no LISTEN. Everything else here says notify -
    a Postgres connection, ``WAKE`` at its default, a notification queued - so
    the driver is the only thing that can answer."""
    monkeypatch.setattr(psycopg_any, "is_psycopg3", False)
    slept: list[float] = []
    connection = FakeConnection(notifications=1)
    assert wait_for_work(0.5, sleep=slept.append, connection=connection) is False
    assert slept == [0.5]
    assert connection.sql == []


def test_a_driver_that_cannot_listen_sends_no_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both halves read one answer, as they do for ``WAKE``: a NOTIFY nothing
    can hear would cost the serialized commit and buy nothing."""
    monkeypatch.setattr(psycopg_any, "is_psycopg3", False)
    connection = FakeConnection()
    notify_relay(connection=connection)
    assert connection.sql == []


def test_psycopg2_polls_on_a_real_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same on the server, through the driver Django actually loaded rather
    than one stood in for it."""
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
