from __future__ import annotations

import os
import signal
import socket
from collections.abc import Callable
from types import FrameType, TracebackType
from typing import Any

from django.core.management.base import BaseCommand, OutputWrapper

from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.delivery.run_relay import run_relay

# What a container runtime sends to ask for a stop, and what Ctrl-C sends.
_STOPPING = (signal.SIGTERM, signal.SIGINT)


class Command(BaseCommand):
    help = "Deliver outbox rows: one pass with --once, or run as a relay."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--once", action="store_true", help="One pass, then exit.")
        parser.add_argument("--limit", type=int, default=None, help="Deliver at most this many.")
        parser.add_argument("--passes", type=int, default=None, help="Stop after this many passes.")
        parser.add_argument("--worker-id", default=None, help="Defaults to host:pid.")

    def handle(self, *args: Any, **options: Any) -> None:
        worker_id = options["worker_id"] or f"{socket.gethostname()}:{os.getpid()}"
        if options["once"]:
            counts = deliver_pending(limit=options["limit"], worker_id=worker_id)
        else:
            with _StopOnSignal(self.stderr) as stop:
                counts = run_relay(worker_id=worker_id, passes=options["passes"], stop=stop)
        if not counts:
            self.stdout.write("Nothing owed.")
            return
        for status, count in sorted(counts.items(), key=lambda pair: pair[0].value):
            self.stdout.write(f"{status.value}: {count}")


class _StopOnSignal:
    """SIGTERM and SIGINT ask the relay to stop; a second one ends it at once.

    The first signal only sets a flag. The relay reads it between deliveries,
    so the receiver running when it arrives finishes and commits, and the rest
    of the claimed batch is handed back for another worker to take at once.
    Nothing is logged from the handler itself: it runs between two bytecodes of
    whatever the main thread was doing, and the relay reports the stop when it
    acts on it.

    A second signal raises ``SystemExit``, a ``BaseException``, because the
    relay deliberately swallows every ``Exception`` a delivery raises and a
    receiver may swallow them too. The receiver's transaction rolls back with
    it, and its row is reclaimed when the lease lapses.

    The handlers that were installed before are put back on the way out,
    however the relay ends, so a command run inside a larger process does not
    leave that process's Ctrl-C rewired.

    Python installs signal handlers from the main thread only, and raises
    ``ValueError`` anywhere else. A relay started from another thread runs
    without a stop, and says so, rather than refusing to run: whoever started
    the thread owns stopping it.
    """

    def __init__(self, stderr: OutputWrapper) -> None:
        self.stderr = stderr
        self.requested = False
        self.previous: dict[int, Any] = {}

    def __enter__(self) -> Callable[[], bool]:
        try:
            for signum in _STOPPING:
                self.previous[signum] = signal.signal(signum, self._handle)
        except ValueError:
            self.stderr.write(
                "deliver_events is not on the main thread, so it cannot handle "
                "SIGTERM or SIGINT; it will run until --passes is spent or the "
                "thread that started it ends it."
            )
        return lambda: self.requested

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        if self.requested:
            raise SystemExit(128 + signum)
        self.requested = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        for signum, previous in self.previous.items():
            # ``getsignal`` reports None for a handler installed outside Python,
            # which ``signal`` will not take back; the default is the nearest
            # thing to restore. SIG_DFL is zero, so ``or`` keeps it as it is.
            signal.signal(signum, previous or signal.SIG_DFL)
