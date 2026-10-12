"""The system check on ``AROUND_DELIVERY``.

Beside ``test_checks.py`` rather than in it, so the lanes adding settings in one
release do not edit one file. Every case goes through the public
``check_settings_keys_are_known``, which is where the check is folded in.
"""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any

import pytest

from django_domain_events import checks

#: Imports, and is not callable.
NOT_CALLABLE = "a constant, not a factory"


def hook(context: Any) -> nullcontext[None]:
    return nullcontext()


def test_no_hook_is_clean(settings: Any) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"AROUND_DELIVERY": None}
    assert checks.check_settings_keys_are_known() == []


def test_a_hook_that_imports_and_is_callable_is_clean(settings: Any) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {"AROUND_DELIVERY": f"{__name__}.hook"}
    assert checks.check_settings_keys_are_known() == []


@pytest.mark.parametrize(
    ("value", "says"),
    [
        (f"{__name__}.no_such_hook", "cannot be imported"),
        ("no_such_module.hook", "cannot be imported"),
        (f"{__name__}.NOT_CALLABLE", "is not callable"),
        (hook, "is not a dotted path"),
    ],
    ids=["missing-attribute", "missing-module", "not-callable", "not-a-string"],
)
def test_a_hook_that_cannot_be_used_is_an_error(settings: Any, value: object, says: str) -> None:
    """One case per arm of the check: the import, the callable test, and the
    type test, without which ``import_string`` would raise out of the check
    itself rather than report. The callable itself is the likeliest mistake,
    since every other collaborator in a settings file is an object."""
    settings.DJANGO_DOMAIN_EVENTS = {"AROUND_DELIVERY": value}

    problems = checks.check_settings_keys_are_known()

    assert [(p.level, p.id) for p in problems] == [(40, "django_domain_events.E011")]
    assert "AROUND_DELIVERY" in problems[0].msg
    assert says in problems[0].msg
