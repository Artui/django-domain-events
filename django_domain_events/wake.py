from __future__ import annotations

import time as time_module
from collections.abc import Callable
from typing import Any

from django.db import connections

from django_domain_events.write_alias import write_alias

CHANNEL = "django_domain_events"


def notify_relay(*, supported: bool | None = None, connection: Any | None = None) -> None:
    """Tell a listening relay that something is owed.

    Fire-and-forget: a notification sent while nobody is listening is simply
    lost, which is why the relay's poll stays as the floor rather than being
    replaced by this. It removes latency and never carries the obligation - the
    delivery row does.
    """
    connection = connection or connections[write_alias()]
    if not _resolve(supported, connection):
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
    return supported if supported is not None else connection.vendor == "postgresql"


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
