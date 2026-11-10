from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import pytest
from django.core.checks import Tags
from django.core.checks.registry import registry as check_registry

from django_domain_events import checks


def test_every_check_the_package_defines_is_registered() -> None:
    """A check written but not registered in AppConfig.ready never runs."""
    defined = {
        function
        for name, function in inspect.getmembers(checks, inspect.isfunction)
        if name.startswith("check_") and function.__module__ == checks.__name__
    }
    registered = set(check_registry.get_checks(include_deployment_checks=True))
    assert defined
    assert defined - registered == set()


@pytest.mark.parametrize(
    "check",
    [
        checks.check_no_orphaned_deliveries,
        checks.check_recorded_events_are_declared,
        checks.check_database_measures_characters,
    ],
    ids=lambda check: check.__name__,
)
def test_a_check_that_queries_the_database_is_registered_under_its_tag(
    check: Callable[..., Any],
) -> None:
    """Django runs database-tagged checks only where a database is asked about."""
    assert check in check_registry.get_checks(include_deployment_checks=True)
    assert Tags.database in check.tags
