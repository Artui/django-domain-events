from __future__ import annotations

from datetime import datetime, timezone

from django.utils import timezone as django_timezone


class NotYet(Exception):
    """Raised by a durable receiver to say its work is not due until ``until``.

    The answer to a delivery that runs early. A receiver declared with ``due=``
    has its due time resolved when the event fires, and the world moves on
    after that: an admin extends an offer from 48 hours to 72, the 48-hour row
    runs, finds the offer still open - and returning would succeed and leave
    nothing that ever expires it. Raising this instead moves the row to
    ``until`` and lets it run again then, so the package does not depend on
    the application remembering to fire a follow-up event for every change
    that moves a due time.

    The relay writes ``until`` to the row's ``available_at`` and ``due_at``
    together, sets it back to ``PENDING`` and does nothing else:

    - **No attempt is spent**, and the next run is handed the same attempt
      number, because nothing failed. ``max_attempts`` cannot end it.
    - **It is not capped** by ``MAX_RECEIVER_RETRY_DELAY_SECONDS``, and not
      bounded by ``give_up_after``. Those exist because a destination's advice
      about its own readiness cannot be trusted to be sane; this is the
      application's own declaration of when its work is due, as ``due=`` is.
      ``outbox_health()`` shows the row as scheduled until then.
    - **It is not a failure**: ``last_error`` is not written and
      ``on_failure`` is not called.
    - **It sets nothing else aside.** A ``RetryAfter(counts=False)`` pauses its
      lane because a throttle limits every row behind it; this is about one
      row, and the rest of the batch is delivered.

    ``DeliveryContext.due_at`` on the next run is ``until``, which is what the
    receiver compares against: recompute the due time from current state, and
    go ahead once it is no later than the context's.

    ``until`` is an aware datetime, and in the future when this is
    constructed. A naive one is refused, as ``due=`` refuses one, and so is a
    time already come: spending no attempt, the row would be claimed straight
    back and answered the same way, a hot loop nothing would end. Both are
    refused where the exception is built, so a receiver raising either has
    raised an ordinary ``TypeError`` or ``ValueError`` instead, which the relay
    records as an ordinary failure, counted, with that message.

    Not a ``RetryAfter`` spelling, although it rests on the same machinery -
    an outcome read at the one boundary every execution site shares, recorded
    without touching ``attempts``. ``RetryAfter`` is a destination's advice,
    so it is counted by default, capped, jittered, bounded by
    ``give_up_after`` and pauses its lane; this is none of those, and a mode of
    that class switching each of them off would be more to learn than a class
    with one behaviour.

    Only a durable receiver can be rescheduled: the relay reads it and nothing
    else does. An ``INLINE`` receiver raising it fails the caller's transaction
    like any other exception
    (``test_an_inline_receiver_raising_it_fails_the_callers_change``), and an
    ``ON_COMMIT`` one has it logged
    (``test_an_on_commit_receiver_raising_it_has_it_logged``).
    """

    def __init__(self, until: datetime, reason: str = "") -> None:
        if not isinstance(until, datetime):
            raise TypeError(
                f"NotYet needs a datetime, got {until!r}, a {type(until).__name__}. "
                f"A delay is RetryAfter's; this takes the time the work is due."
            )
        if not django_timezone.is_aware(until):
            raise ValueError(
                f"NotYet needs an aware datetime, got the naive {until!r}. Reading "
                f"it as UTC or as local time would each be a guess."
            )
        if not until > datetime.now(timezone.utc):
            raise ValueError(
                f"NotYet needs a time in the future, got {until.isoformat()}. The row "
                f"would be due again at once, and spending no attempt, nothing would "
                f"end the loop."
            )
        self.until = until
        self.reason = reason
        super().__init__(reason or f"not due until {until.isoformat()}")
