"""Reading this package's settings, with the defaults in one place."""

from __future__ import annotations

from typing import Any

from django.conf import settings
from django.utils.module_loading import import_string

from django_domain_events.codecs.payload_codec import PayloadCodec

SETTINGS_NAME = "DJANGO_DOMAIN_EVENTS"

DEFAULTS: dict[str, Any] = {
    "CODEC": "django_domain_events.codecs.dataclass_codec.DataclassCodec",
    "WARN_OUTSIDE_ATOMIC": True,
}


def setting(key: str) -> Any:
    """Read one setting, falling back to this package's default."""
    configured = getattr(settings, SETTINGS_NAME, {})
    return configured.get(key, DEFAULTS[key])


def get_codec() -> PayloadCodec:
    """Build the configured codec.

    Imported by dotted path and instantiated per call rather than cached at
    module import: caching it would freeze whatever was configured the first
    time any event fired, which makes a test that overrides the setting pass or
    fail depending on what ran before it.

    Named in settings rather than sniffed from what happens to be installed. A
    codec that picks itself means the same event class decodes on one machine
    and raises on another, and the divergence is silent.
    """
    codec_class = import_string(setting("CODEC"))
    return codec_class()
