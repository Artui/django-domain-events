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
    backend. Otherwise it takes both a Postgres connection and ``WAKE =
    "notify"``; ``WAKE = "poll"`` is the switch for a deployment that would
    rather not pay a serialized commit per ``fire()`` (see the operations page).
    """
    if supported is not None:
        return supported
    return connection.vendor == "postgresql" and setting("WAKE") == "notify"


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
    """
    with connection.cursor() as cursor:
        cursor.execute(f'LISTEN "{CHANNEL}"')
    driver = connection.connection
    for _ in driver.notifies(timeout=timeout, stop_after=1):
        return True
    return False
