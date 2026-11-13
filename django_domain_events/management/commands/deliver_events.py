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
from django_domain_events.settings import get_relay_startup
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
            # None, not the default lane: an append would otherwise add to that default too.
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
        # The relay has no total to cap, so a --limit beside it would be read by nothing.
        lane: str | list[str] = lanes[0] if len(lanes) == 1 else lanes
        lane_batch_sizes = _lane_batch_sizes(options["lane_batch_size"])
        if options["limit"] is not None and not options["once"]:
            raise CommandError(
                "--limit caps a single pass and needs --once; the relay delivers until "
                "it is stopped. Bound a relay with --passes, or size its claims with "
                "--batch-size."
            )
        startup = get_relay_startup()
        if startup is not None:
            startup(worker_id=worker_id, lanes=list(lanes))
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
    """Each ``--lane-batch-size LANE=N``, as the mapping ``lane_batch_sizes`` takes."""
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
    """SIGTERM and SIGINT ask the relay to stop; a second one ends it at once."""

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
            # A None handler from getsignal cannot be restored, so the default stands in for it.
            signal.signal(signum, previous or signal.SIG_DFL)
