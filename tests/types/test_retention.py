from __future__ import annotations

import django_domain_events
from django_domain_events.types.retention import Retention


def test_the_values_are_the_ones_already_written_to_event_rows() -> None:
    """Renaming one strands rows recorded under the old spelling."""
    assert {policy.name: policy.value for policy in Retention} == {
        "SUCCEEDED": "succeeded",
        "SETTLED": "settled",
    }


def test_it_is_exported_from_the_package_root() -> None:
    assert django_domain_events.Retention is Retention
