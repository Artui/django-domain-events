from __future__ import annotations

from datetime import datetime

from django.utils import timezone

from django_domain_events.utils import in_project_time, require_aware_where_stored_aware


class NotYet(Exception):
    """Raised by a durable receiver to say its work is not due until ``until``."""

    def __init__(self, until: datetime, reason: str = "") -> None:
        if not isinstance(until, datetime):
            raise TypeError(
                f"NotYet needs a datetime, got {until!r}, a {type(until).__name__}. "
                f"A delay is RetryAfter's; this takes the time the work is due."
            )
        require_aware_where_stored_aware(
            until,
            f"NotYet needs an aware datetime, got the naive {until!r}. Under "
            f"USE_TZ = True reading it as UTC or as local time would each be a guess.",
        )
        until = in_project_time(until)
        if not until > timezone.now():
            raise ValueError(
                f"NotYet needs a time in the future, got {until.isoformat()}. The row "
                f"would be due again at once, and spending no attempt, nothing would "
                f"end the loop."
            )
        self.until = until
        self.reason = reason
        super().__init__(reason or f"not due until {until.isoformat()}")
