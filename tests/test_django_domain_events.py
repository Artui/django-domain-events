"""Tests mirroring ``django_domain_events/``.

One file per source file, same name. This one covers the package root.
"""

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
