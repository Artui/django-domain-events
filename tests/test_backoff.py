from __future__ import annotations

from datetime import timedelta

import pytest

from django_domain_events.backoff import backoff


def test_the_ceiling_doubles_with_each_attempt() -> None:
    """Asserted as a curve rather than sampled: jitter is an argument, so the
    schedule is a pure function and the test does not have to draw from it."""
    ceilings = [backoff(n, base=2.0, cap=3600.0, jitter=1.0).total_seconds() for n in range(1, 6)]
    assert ceilings == [2.0, 4.0, 8.0, 16.0, 32.0]


def test_the_cap_holds() -> None:
    assert backoff(20, base=2.0, cap=60.0, jitter=1.0) == timedelta(seconds=60)


def test_jitter_draws_from_the_whole_window() -> None:
    """Full jitter, not a ceiling plus a bit. Retrying at ceiling-plus-noise
    keeps every failed delivery in one cohort, which is the herd the backoff
    exists to break up."""
    assert backoff(3, base=2.0, cap=3600.0, jitter=0.0) == timedelta(0)
    assert backoff(3, base=2.0, cap=3600.0, jitter=0.5) == timedelta(seconds=4)


def test_attempts_are_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        backoff(0, base=2.0, cap=60.0, jitter=1.0)
