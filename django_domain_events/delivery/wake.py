from __future__ import annotations

import threading
import time as time_module
from collections.abc import Callable
from typing import Any

from django.db import connections

from django_domain_events.delivery.utils import driver_listens
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
    """Tell a listening relay that something is owed."""
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
    """Block until a notification arrives or ``timeout`` elapses."""
    connection = connection or connections[write_alias()]
    if not _resolve(supported, connection):
        sleep(timeout)
        return False
    return _wait_on(connection, timeout)


def _resolve(supported: bool | None, connection: Any) -> bool:
    """Whether this process notifies and listens at all."""
    if supported is not None:
        return supported
    return connection.vendor == "postgresql" and setting("WAKE") == "notify" and driver_listens()


def _coalesced(connection: Any, clock: Callable[[], float]) -> bool:
    """Whether a NOTIFY went out too recently for this one to add anything."""
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
    """Listen on the channel and wait for one notification."""
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
