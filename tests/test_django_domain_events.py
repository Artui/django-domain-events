"""Tests mirroring ``django_domain_events/``.

One file per source file, same name. This one covers the package root.
"""

import pytest

import django_domain_events
from django_domain_events.version import __version__


def test_version_is_exported_from_the_package_root() -> None:
    """The root re-exports the version, which is the only public name so far.

    Asserting identity rather than a literal: the point is that ``__init__``
    and ``version.py`` cannot drift apart, not that the number is any
    particular value on the day this runs.
    """
    assert django_domain_events.__version__ is __version__


def test_version_is_a_dotted_release_string() -> None:
    """``bump-my-version`` rewrites this string by pattern, and the release
    script parses it back out, so its shape is load-bearing rather than
    cosmetic."""
    parts = __version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


def test_the_public_surface_is_importable() -> None:
    """Every name in ``__all__`` resolves."""
    import django_domain_events

    for name in django_domain_events.__all__:
        assert getattr(django_domain_events, name) is not None


def test_the_symbol_wins_over_the_submodule_of_the_same_name() -> None:
    """Under one-symbol-per-file the module and the symbol share a name, and
    importing the submodule binds the *module* as an attribute of the package.

    This is the regression test for a real consumer-facing bug: with a lazy PEP
    562 re-export, this package's own ``from django_domain_events.fire import
    context_for`` rebound the attribute, and ``from django_domain_events import
    fire`` then handed back a module object.
    """
    import django_domain_events
    import django_domain_events.fire

    assert callable(django_domain_events.fire)
    assert not hasattr(django_domain_events.fire, "__file__")


def test_an_unknown_name_raises_attribute_error() -> None:
    import django_domain_events

    with pytest.raises(AttributeError, match="nope"):
        _ = django_domain_events.nope
