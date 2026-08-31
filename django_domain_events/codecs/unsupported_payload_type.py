"""Raised when a codec is asked for something it will not approximate."""

from __future__ import annotations


class UnsupportedPayloadType(TypeError):
    """The configured codec cannot honour a field's declared type.

    Raised at declaration time where possible, and at decode time otherwise. A
    generated value that is wrong is worse than one that refuses to exist: a
    best-effort decode puts a dict where a consumer's annotation promised a
    dataclass, and the failure then surfaces somewhere unrelated.
    """
