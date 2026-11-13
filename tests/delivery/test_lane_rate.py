from __future__ import annotations

import pytest

from django_domain_events.delivery.lane_rate import LaneRate


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_the_first_start_waits_for_nothing() -> None:
    assert LaneRate(5, Clock()).wait_time() == 0


def test_the_next_start_waits_the_rest_of_one_interval() -> None:
    clock = Clock()
    gate = LaneRate(5, clock)
    gate.started()
    clock.now = 0.05
    assert gate.wait_time() == pytest.approx(0.15)


def test_the_wait_is_zero_once_the_interval_has_passed() -> None:
    clock = Clock()
    gate = LaneRate(5, clock)
    gate.started()
    clock.now = 0.2
    assert gate.wait_time() == 0
    clock.now = 9.0
    assert gate.wait_time() == 0


def test_a_late_start_spaces_the_next_from_when_it_began() -> None:
    clock = Clock()
    gate = LaneRate(1, clock)
    gate.started()
    clock.now = 5.0
    gate.started()
    clock.now = 5.25
    assert gate.wait_time() == pytest.approx(0.75)


def test_five_a_second_over_ten_seconds_never_exceeds_five_in_a_window() -> None:
    clock = Clock()
    gate = LaneRate(5, clock)
    starts: list[float] = []
    while clock.now < 10:
        clock.now += gate.wait_time()
        gate.started()
        starts.append(clock.now)
    for begin in starts:
        assert len([s for s in starts if begin <= s < begin + 1 - 1e-9]) <= 5
    assert len(starts) >= 50


def test_half_a_start_a_second_spaces_starts_two_seconds_apart() -> None:
    clock = Clock()
    gate = LaneRate(0.5, clock)
    starts: list[float] = []
    for _ in range(4):
        clock.now += gate.wait_time()
        gate.started()
        starts.append(clock.now)
    assert starts == [0.0, 2.0, 4.0, 6.0]


@pytest.mark.parametrize("rate", [0, -1, -0.5])
def test_a_rate_that_is_not_positive_is_refused(rate: float) -> None:
    with pytest.raises(ValueError, match="greater than 0"):
        LaneRate(rate, Clock())
