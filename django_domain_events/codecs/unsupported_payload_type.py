from __future__ import annotations


class UnsupportedPayloadType(TypeError):
    """The configured codec cannot honour the declared type of a field."""
