from __future__ import annotations

import inspect

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
