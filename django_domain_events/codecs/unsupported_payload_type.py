"""Raised when a codec is asked for something it will not approximate."""

from __future__ import annotations


class UnsupportedPayloadType(TypeError):
    """The configured codec cannot honour a field's declared type.

    A best-effort decode would put a dict where an annotation promised a
    dataclass, and the failure would then surface somewhere unrelated.
    """
