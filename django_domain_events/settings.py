from __future__ import annotations

from typing import Any

from django.conf import settings
from django.utils.module_loading import import_string

from django_domain_events.codecs.payload_codec import PayloadCodec

SETTINGS_NAME = "DJANGO_DOMAIN_EVENTS"

DEFAULTS: dict[str, Any] = {
    "CODEC": "django_domain_events.codecs.dataclass_codec.DataclassCodec",
    "WARN_OUTSIDE_ATOMIC": True,
    "BATCH_SIZE": 50,
    "LEASE_SECONDS": 300,
    "POLL_SECONDS": 1.0,
    "BACKOFF_BASE_SECONDS": 2.0,
    "BACKOFF_CAP_SECONDS": 3600.0,
    "RETENTION_DAYS": 90,
    "TASK_BACKEND": None,
}


def setting(key: str) -> Any:
    configured = getattr(settings, SETTINGS_NAME, {})
    return configured.get(key, DEFAULTS[key])


def get_task_backend() -> Any | None:
    """The configured task backend, or None when receivers run in the relay.

    Accepts a dotted path, or a mapping with ``BACKEND`` and the rest passed to
    the constructor - otherwise a backend with any options at all is
    unreachable through the documented setting and only a subclass can use it.
    """
    configured = setting("TASK_BACKEND")
    if configured is None:
        return None
    if isinstance(configured, str):
        return import_string(configured)()
    options = dict(configured)
    return import_string(options.pop("BACKEND"))(**options)


def get_codec() -> PayloadCodec:
    """Build the configured codec.

    Not cached: caching would freeze whatever was configured when the first
    event fired, so a test overriding the setting would pass or fail on what ran
    before it. Named in settings rather than sniffed from what is installed,
    because a codec that picks itself decodes on one machine and raises on
    another, silently.
    """
    return import_string(setting("CODEC"))()
