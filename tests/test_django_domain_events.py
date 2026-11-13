from __future__ import annotations

import pytest

import django_domain_events
from django_domain_events.version import __version__


def test_version_is_exported_from_the_package_root() -> None:
    """The root re-exports the version, which is the only public name so far."""
    assert django_domain_events.__version__ is __version__


def test_version_is_a_dotted_release_string() -> None:
    """bump-my-version rewrites this string by pattern, so its shape is load-bearing."""
    parts = __version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)


def test_the_public_surface_is_importable() -> None:
    """Every name in ``__all__`` resolves."""
    import django_domain_events

    for name in django_domain_events.__all__:
        assert getattr(django_domain_events, name) is not None


def test_the_symbol_wins_over_the_submodule_of_the_same_name() -> None:
    """Importing the submodule binds the module, not the symbol, on the package."""
    import django_domain_events
    import django_domain_events.delivery.fire

    assert callable(django_domain_events.fire)
    assert not hasattr(django_domain_events.fire, "__file__")

    from types import ModuleType

    assert isinstance(django_domain_events.delivery.fire, ModuleType)


def test_an_unknown_name_raises_attribute_error() -> None:
    import django_domain_events

    with pytest.raises(AttributeError, match="nope"):
        _ = django_domain_events.nope


def test_every_name_the_root_binds_is_in_all_in_sorted_order() -> None:
    """A name imported or assigned in ``__init__`` but left out of ``__all__`` is lost to ``*``."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(django_domain_events))
    bound = {
        alias.asname or alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    bound |= {
        target.id
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id != "__all__"
    }

    assert set(django_domain_events.__all__) == bound
    assert django_domain_events.__all__ == sorted(django_domain_events.__all__)
