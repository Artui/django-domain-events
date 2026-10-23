from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import pytest
from django.core.checks import Tags
from django.core.checks.registry import registry as check_registry

from django_domain_events import checks


def test_every_check_the_package_defines_is_registered() -> None:
    """A check is registered by hand in ``AppConfig.ready``, one line each, and a
    check written and tested but never registered is one Django never runs:
    every test of it calls the function directly and passes. That is how the
    psycopg2 warning first went in. So the registration is held here against
    the module rather than check by check, and a new ``check_*`` function fails
    this until its line is added."""
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
    """Django runs a check under the database tag only where it is asked about
    a database, as ``migrate`` and ``check --database`` ask, so a plain
    ``check`` on a host with no database reachable still runs. Registered
    without the tag, the check would be run by every ``check`` and these
    functions' own ``databases`` guards would be all that kept it quiet:
    every test calling the function directly would pass either way."""
    assert check in check_registry.get_checks(include_deployment_checks=True)
    assert Tags.database in check.tags
