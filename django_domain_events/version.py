"""The single source of truth for this package's version.

``pyproject.toml`` declares ``dynamic = ["version"]`` and reads this file through
``[tool.hatch.version]``, so there is exactly one string to bump and nothing to
keep in sync.
"""

from __future__ import annotations

__version__: str = "0.1.0"

__all__ = ["__version__"]
