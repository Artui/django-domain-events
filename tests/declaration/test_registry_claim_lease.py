"""``Registry.claim_lease_seconds``: the lease a claim stamps.

On a registry of its own, so the test app's receivers are not in the count.
"""

from __future__ import annotations

from typing import Any

from django_domain_events.declaration.registry import LEASE_HEADROOM, Registry
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.registered_receiver import DEFAULT_LANE, RegisteredReceiver
from tests.testapp.events import Unheard

SETTING = 300


def _registry(*receivers: dict[str, Any]) -> Registry:
    registry = Registry()
    for number, options in enumerate(receivers):
        registry.register_receiver(
            RegisteredReceiver(
                key=f"probe.{number}",
                event_class=Unheard,
                func=lambda event: None,
                mode=options.pop("mode", DeliveryMode.DURABLE),
                takes_context=False,
                max_attempts=5,
                eager=options.pop("eager", False),
                site="relay",
                **options,
            )
        )
    return registry


def test_the_headroom_is_ten_percent() -> None:
    """The stamp a uniform 60 s lane gets, and the band's upper bound for it."""
    assert round(60 * LEASE_HEADROOM, 6) == 66


def test_a_lane_sharing_one_lease_is_claimed_under_it() -> None:
    registry = _registry(
        {"lane": "mail", "lease_seconds": 60}, {"lane": "mail", "lease_seconds": 60}
    )
    assert registry.claim_lease_seconds("mail", default=SETTING) == 60 * LEASE_HEADROOM


def test_a_lane_mixing_leases_is_claimed_under_the_default() -> None:
    registry = _registry(
        {"lane": "mail", "lease_seconds": 60}, {"lane": "mail", "lease_seconds": 90}
    )
    assert registry.claim_lease_seconds("mail", default=SETTING) == SETTING * LEASE_HEADROOM


def test_a_receiver_declaring_no_lease_counts_as_the_default() -> None:
    """Sixty and the setting are a mix; the setting twice is not."""
    mixed = _registry({"lane": "mail", "lease_seconds": 60}, {"lane": "mail"})
    assert mixed.claim_lease_seconds("mail", default=SETTING) == SETTING * LEASE_HEADROOM
    same = _registry({"lane": "mail", "lease_seconds": SETTING}, {"lane": "mail"})
    assert same.claim_lease_seconds("mail", default=SETTING) == SETTING * LEASE_HEADROOM


def test_no_durable_receiver_is_claimed_under_the_default() -> None:
    assert Registry().claim_lease_seconds(None, default=SETTING) == SETTING * LEASE_HEADROOM


def test_a_receiver_that_writes_no_rows_does_not_count() -> None:
    """An inline receiver declaring a lease has no rows for a claim to take."""
    registry = _registry(
        {"lane": "mail", "lease_seconds": 60},
        {"lane": "mail", "lease_seconds": 10, "mode": DeliveryMode.INLINE},
    )
    assert registry.claim_lease_seconds("mail", default=SETTING) == 60 * LEASE_HEADROOM


def test_a_named_lane_counts_only_its_own_receivers() -> None:
    registry = _registry(
        {"lane": "mail", "lease_seconds": 60}, {"lane": "reports", "lease_seconds": 1800}
    )
    assert registry.claim_lease_seconds("mail", default=SETTING) == 60 * LEASE_HEADROOM
    assert registry.claim_lease_seconds("reports", default=SETTING) == 1800 * LEASE_HEADROOM


def test_the_default_lane_counts_the_receivers_in_no_named_lane() -> None:
    registry = _registry({"lease_seconds": 60}, {"lane": "reports", "lease_seconds": 1800})
    assert registry.claim_lease_seconds(DEFAULT_LANE, default=SETTING) == 60 * LEASE_HEADROOM


def test_every_lane_counts_every_receiver() -> None:
    registry = _registry({"lease_seconds": 60}, {"lane": "reports", "lease_seconds": 60})
    assert registry.claim_lease_seconds(None, default=SETTING) == 60 * LEASE_HEADROOM
    registry = _registry({"lease_seconds": 60}, {"lane": "reports", "lease_seconds": 1800})
    assert registry.claim_lease_seconds(None, default=SETTING) == SETTING * LEASE_HEADROOM


def test_the_eager_attempt_counts_only_eager_receivers() -> None:
    """The attempt after commit claims only eager receivers' rows, and a lane
    does not bound it: it runs whatever lane its receivers are in."""
    registry = _registry(
        {"eager": True, "lane": "mail", "lease_seconds": 60},
        {"eager": True, "lease_seconds": 60},
        {"lease_seconds": 1800},
    )
    assert registry.claim_lease_seconds(None, default=SETTING, eager=True) == 60 * LEASE_HEADROOM
    assert registry.claim_lease_seconds(None, default=SETTING) == SETTING * LEASE_HEADROOM


def test_eager_receivers_mixing_leases_are_claimed_under_the_default() -> None:
    """The eager attempt knows its rows' ids and not their receivers, so its base
    is every eager receiver's: a mix there costs the skip, never correctness,
    since the band is decided per row against the row's own receiver."""
    registry = _registry({"eager": True, "lease_seconds": 60}, {"eager": True, "lease_seconds": 90})
    assert registry.claim_lease_seconds(None, default=SETTING, eager=True) == (
        SETTING * LEASE_HEADROOM
    )
