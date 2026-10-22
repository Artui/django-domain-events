from __future__ import annotations

import threading
import time as time_module
from collections.abc import Callable
from typing import Any

from django.db import connections

from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.settings import setting

CHANNEL = "django_domain_events"

# When this process last sent a NOTIFY, per database alias: a relay listens on
# one database, so a NOTIFY on ``default`` says nothing to the relay on
# ``events`` and must not stand in for it. Process state, so tests reset it.
_last_sent: dict[str, float] = {}
_lock = threading.Lock()


def notify_relay(
    *,
    supported: bool | None = None,
    connection: Any | None = None,
    clock: Callable[[], float] = time_module.monotonic,
) -> None:
    """Tell a listening relay that something is owed.

    Fire-and-forget: a notification sent while nobody is listening is simply
    lost, which is why the relay's poll stays as the floor rather than being
    replaced by this. It removes latency and never carries the obligation - the
    delivery row does.

    Sends nothing under ``WAKE = "poll"``, and at most one per
    ``NOTIFY_COALESCE_SECONDS`` per database from this process. Skipping one is
    safe for the same reason losing one is: a single wake makes the relay claim
    everything due, and the poll picks up whatever the skipped one announced.
    ``clock`` is injected, as ``sleep`` is below, so a test moves time rather
    than waiting for it.
    """
    connection = connection or connections[write_alias()]
    if not _resolve(supported, connection):
        return
    if _coalesced(connection, clock):
        return
    with connection.cursor() as cursor:
        cursor.execute(f'NOTIFY "{CHANNEL}"')


def wait_for_work(
    timeout: float,
    *,
    supported: bool | None = None,
    sleep: Callable[[float], None] = time_module.sleep,
    connection: Any | None = None,
) -> bool:
    """Block until a notification arrives or ``timeout`` elapses.

    Returns whether it was woken. Falls back to sleeping where the backend
    cannot notify, so the relay loop reads the same on every database.

    ``supported`` and ``connection`` are arguments rather than only probes, so
    both branches and the statements they issue are reachable from either
    backend. Otherwise each could only be covered by the database that has it,
    and the SQL itself would never be asserted anywhere.
    """
    connection = connection or connections[write_alias()]
    if not _resolve(supported, connection):
        sleep(timeout)
        return False
    return _wait_on(connection, timeout)


def _resolve(supported: bool | None, connection: Any) -> bool:
    """Whether this process notifies and listens at all.

    An explicit ``supported`` wins, so both branches stay reachable from either
    backend. Otherwise it takes a Postgres connection, ``WAKE = "notify"``, and
    a driver that can listen; ``WAKE = "poll"`` is the switch for a deployment
    that would rather not pay a serialized commit per ``fire()`` (see the
    operations page).

    The driver has to be psycopg 3. The wait blocks in its ``notifies()``
    generator, and psycopg2's ``notifies`` is a list filled by ``poll()``, so
    under psycopg2 the relay polls and nothing sends a NOTIFY no relay could
    hear. A system check says so where ``WAKE`` still asks for notify.

    Neither the ``WAKE`` nor the driver conjunct is visible to the coverage gate
    (one arc). ``WAKE`` is held by ``test_polling_sends_no_notification`` and
    ``test_polling_does_not_listen_either``, the driver by
    ``test_a_driver_that_cannot_listen_polls`` and
    ``test_a_driver_that_cannot_listen_sends_no_notification``.
    """
    if supported is not None:
        return supported
    return connection.vendor == "postgresql" and setting("WAKE") == "notify" and _driver_listens()


def _driver_listens() -> bool:
    """Whether Django's Postgres backend loaded psycopg 3.

    Imported here, and only once the connection is known to be Postgres:
    ``psycopg_any`` imports psycopg 3 or else psycopg2 and raises if neither is
    installed, which a project on another database never has. Its backend has
    already imported the module by the time a connection names Postgres, so
    this is a lookup rather than an import. Read at call time, so a test can
    stand either driver in on any backend.
    """
    from django.db.backends.postgresql import psycopg_any

    return psycopg_any.is_psycopg3


def _coalesced(connection: Any, clock: Callable[[], float]) -> bool:
    """Whether a NOTIFY went out too recently for this one to add anything.

    Otherwise records this one as sent. Two conjuncts, neither visible to the
    coverage gate, which counts the chain as one arc: ``last is not None`` is held
    by ``test_notify_is_the_default_on_postgres`` (the first send would raise on
    the subtraction), and ``now - last < interval`` by
    ``test_a_notification_after_the_interval_sends_another``.

    A zero interval needs no conjunct of its own: elapsed time on a monotonic
    clock is never negative, so ``< 0`` is never true. Changing ``<`` to ``<=``
    is what ``test_a_zero_interval_turns_coalescing_off`` catches.

    The time is taken before the statement runs, so a NOTIFY that then fails
    costs at most one interval of latency, which the poll covers.
    """
    interval = setting("NOTIFY_COALESCE_SECONDS")
    alias = connection.alias
    with _lock:
        now = clock()
        last = _last_sent.get(alias)
        if last is not None and now - last < interval:
            return True
        _last_sent[alias] = now
    return False


def _wait_on(connection: Any, timeout: float) -> bool:
    """Listen on the channel and wait for one notification.

    Reaches for the driver connection, which is the one thing here the ORM does
    not express. Autocommit is required: LISTEN inside a transaction only starts
    delivering once that transaction commits, so a relay holding one would wait
    for a notification it has made itself unable to receive.

    One wake stands for every notification queued behind it. A relay busy
    delivering is not waiting, and psycopg (from 3.2.4) keeps the
    notifications that arrive during other statements in a backlog. The first
    call empties it: psycopg yields the whole backlog before it looks at
    ``stop_after``, and the loop runs the generator to its end
    (``test_notifications_queued_while_busy_wake_the_relay_once``, on Postgres).
    The wake claims everything due, so the queued ones announce nothing it will
    not find.

    The second, zero-timeout call is for what the first stopped short of: a
    notification that reached the socket after the first call's last read.
    Left there, the next wait would return at once for a claim with nothing
    new to find (``test_one_wake_also_takes_what_arrived_after_its_wait_returned``).
    It is a second call made after the first generator is closed, never a
    loop inside it: the open generator holds the connection's lock, which is
    not reentrant, so a nested call would wait on it forever.
    """
    with connection.cursor() as cursor:
        cursor.execute(f'LISTEN "{CHANNEL}"')
    driver = connection.connection
    woken = False
    for _ in driver.notifies(timeout=timeout, stop_after=1):
        woken = True
    if woken:
        for _ in driver.notifies(timeout=0):
            ...
    return woken
