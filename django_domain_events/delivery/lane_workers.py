from __future__ import annotations

import logging
import queue
import threading
import time
from contextvars import Context
from typing import Any

from django.db import Error, connections

from django_domain_events.delivery.deliver import dispatch_one
from django_domain_events.delivery.write_alias import write_alias
from django_domain_events.types.delivery_status import DeliveryStatus

logger = logging.getLogger(__name__)

Result = tuple[int, DeliveryStatus | None, tuple[str, float] | None]


class LaneWorkers:
    """N daemon threads delivering one lane's rows, each on its own connection.

    Not an executor: CPython joins an executor's threads at exit."""

    def __init__(self, lane: str, n: int, worker_id: str, context: Context) -> None:
        self._worker_id = worker_id
        self._context = context
        self._n = n
        self._handoff: queue.SimpleQueue[int | None] = queue.SimpleQueue()
        self._done: queue.SimpleQueue[Result] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._busy = 0
        self._stopped = False
        self._raised: BaseException | None = None
        self._threads = [
            threading.Thread(target=self._work, name=f"{worker_id}-{lane}-{i}", daemon=True)
            for i in range(n)
        ]
        for thread in self._threads:
            thread.start()

    @property
    def failed(self) -> bool:
        """Whether a worker caught a BaseException, which ``stop`` re-raises."""
        return self._raised is not None

    def idle_slots(self) -> int:
        """Rows ``give`` accepts now: none once stopped or failed."""
        if self._stopped or self.failed:
            return 0
        with self._lock:
            return self._n - self._busy

    def give(self, delivery_id: int) -> None:
        """Hand one claimed row to an idle worker."""
        if self._stopped:
            raise RuntimeError("these lane workers are stopped")
        if self.idle_slots() == 0:
            raise RuntimeError("no idle worker to take this row")
        with self._lock:
            self._busy += 1
        self._handoff.put(delivery_id)

    def results(self, timeout: float = 0.0) -> list[Result]:
        """Every ``(id, outcome, deferral)`` reported, waiting up to ``timeout`` s for the first."""
        done: list[Result] = []
        try:
            done.append(self._done.get(block=timeout > 0, timeout=timeout))
        except queue.Empty:
            return done
        while not self._done.empty():
            done.append(self._done.get())
        return done

    def stop(self, deadline: float) -> None:
        """Let rows in flight finish until ``deadline`` (``time.monotonic``), then re-raise."""
        self._stopped = True
        for _ in self._threads:
            self._handoff.put(None)
        for thread in self._threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        if self._raised is not None:
            raise self._raised

    def _work(self) -> None:
        try:
            while (delivery_id := self._handoff.get()) is not None:
                try:
                    result = self._context.copy().run(self._deliver, delivery_id)
                except BaseException as exc:
                    self._raised = self._raised or exc
                    self._report((delivery_id, None, None))
                    return
                self._report(result)
        finally:
            connections.close_all()

    def _deliver(self, delivery_id: int) -> Result:
        deferral: list[tuple[str, float]] = []
        try:
            status = dispatch_one(
                delivery_id,
                worker_id=self._worker_id,
                on_deferral=lambda lane, seconds: deferral.append((lane, seconds)),
            )
        except Exception as exc:
            logger.exception("relay %s could not deliver %s", self._worker_id, delivery_id)
            _close_after(exc, self._worker_id)
            return (delivery_id, None, None)
        return (delivery_id, status, deferral[-1] if deferral else None)

    def _report(self, result: Result) -> None:
        with self._lock:
            self._busy -= 1
        self._done.put(result)


def _close_after(exc: Exception, worker_id: str) -> None:
    """Close on a DB error; raw driver class: test_db_error_closes_own_connection[raw_driver]"""
    connection: Any = connections[write_alias()]
    if not isinstance(exc, (Error, connection.Database.Error)):
        return
    try:
        connection.close()
    except Exception:
        logger.exception("relay %s could not close its worker connection", worker_id)
