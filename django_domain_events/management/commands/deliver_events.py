from __future__ import annotations

import os
import signal
import socket
from collections.abc import Callable
from types import FrameType, TracebackType
from typing import Any

from django.core.management.base import BaseCommand, CommandError, OutputWrapper

from django_domain_events.delivery.deliver import deliver_pending
from django_domain_events.delivery.run_relay import run_relay
from django_domain_events.types.registered_receiver import DEFAULT_LANE

# What a container runtime sends to ask for a stop, and what Ctrl-C sends.
_STOPPING = (signal.SIGTERM, signal.SIGINT)


class Command(BaseCommand):
    help = "Deliver outbox rows: one pass with --once, or run as a relay."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--once", action="store_true", help="One pass, then exit.")
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="With --once, deliver at most this many, shared between the lanes served.",
        )
        parser.add_argument("--passes", type=int, default=None, help="Stop after this many passes.")
        parser.add_argument("--worker-id", default=None, help="Defaults to host:pid.")
        parser.add_argument(
            "--lane",
            action="append",
            # None rather than the default lane: an append adds to its default,
            # so a default of the default lane would make ``--lane mail``
            # serve ``default`` and ``mail``
            # (test_one_lane_alone_is_passed_as_that_lane).
            default=None,
            help=(
                "Serve only the receivers declared with this lane=. Repeat it to "
                "serve several lanes from one process, each claimed in turn. "
                "Defaults to the default lane: every receiver not declared in a "
                "named one."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=None,
            help="Rows per claim for this process. Defaults to BATCH_SIZE.",
        )
        parser.add_argument(
            "--lane-batch-size",
            action="append",
            default=None,
            metavar="LANE=N",
            help=(
                "Rows per claim for one served lane, in place of --batch-size. "
                "Repeat it for each lane to size."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        worker_id = options["worker_id"] or f"{socket.gethostname()}:{os.getpid()}"
        lanes: list[str] = options["lane"] or [DEFAULT_LANE]
        # A lane given once goes through as the string it was, so its path is
        # the one a single lane always took.
        lane: str | list[str] = lanes[0] if len(lanes) == 1 else lanes
        lane_batch_sizes = _lane_batch_sizes(options["lane_batch_size"])
        if options["once"]:
            counts = deliver_pending(
                limit=options["limit"],
                worker_id=worker_id,
                lane=lane,
                batch_size=options["batch_size"],
                lane_batch_sizes=lane_batch_sizes,
            )
        else:
            with _StopOnSignal(self.stderr) as stop:
                counts = run_relay(
                    worker_id=worker_id,
                    passes=options["passes"],
                    stop=stop,
                    lane=lane,
                    batch_size=options["batch_size"],
                    lane_batch_sizes=lane_batch_sizes,
                )
        if not counts:
            self.stdout.write("Nothing owed.")
            return
        for status, count in sorted(counts.items(), key=lambda pair: pair[0].value):
            self.stdout.write(f"{status.value}: {count}")


def _lane_batch_sizes(values: list[str] | None) -> dict[str, int] | None:
    """Each ``--lane-batch-size LANE=N``, as the mapping ``lane_batch_sizes`` takes.

    None when none was given, rather than an empty mapping, because ``--once``
    refuses ``--limit`` beside any ``lane_batch_sizes`` that is not None. Split
    at the last ``=``: a lane name may contain one and N cannot, so ``a=b=5``
    sizes the lane ``a=b``. A size below one, and a lane this process does not
    serve, are left to ``run_relay`` and ``deliver_pending`` to refuse, so the
    command line and Python refuse them alike. Refused here, each by its own
    test: a value with no ``=``, an empty lane, an N that is not an integer,
    and a lane sized twice, even with the same value twice, since two values
    could conflict and the command should not pick one.
    """
    if values is None:
        return None
    sizes: dict[str, int] = {}
    for value in values:
        name, equals, size = value.rpartition("=")
        if not equals:
            raise CommandError(f"--lane-batch-size takes LANE=N, got {value!r}.")
        if not name:
            raise CommandError(f"--lane-batch-size {value!r} names no lane.")
        try:
            rows = int(size)
        except ValueError:
            raise CommandError(
                f"--lane-batch-size {value!r} sizes lane {name!r} with {size!r}, not an integer."
            ) from None
        if name in sizes:
            raise CommandError(f"--lane-batch-size sizes lane {name!r} twice.")
        sizes[name] = rows
    return sizes


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
    it, and its row is reclaimed when the lease lapses. An ``async def``
    receiver's transaction rolls back too, but its coroutine runs on a loop in
    another thread that no signal reaches, and interpreter exit waits for that
    thread: the process leaves once the coroutine returns, or at a third signal.

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
