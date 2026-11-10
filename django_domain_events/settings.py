from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from django.conf import settings
from django.utils.module_loading import import_string

from django_domain_events.codecs.payload_codec import PayloadCodec
from django_domain_events.types.delivery_context import DeliveryContext

SETTINGS_NAME = "DJANGO_DOMAIN_EVENTS"

DEFAULTS: dict[str, Any] = {
    "CODEC": "django_domain_events.codecs.dataclass_codec.DataclassCodec",
    "WARN_OUTSIDE_ATOMIC": True,
    "BATCH_SIZE": 50,
    "LEASE_SECONDS": 300,
    "POLL_SECONDS": 1.0,
    "WAKE": "notify",
    "NOTIFY_COALESCE_SECONDS": 0.5,
    "BACKOFF_BASE_SECONDS": 2.0,
    "BACKOFF_CAP_SECONDS": 3600.0,
    "MAX_RECEIVER_RETRY_DELAY_SECONDS": 86400.0,
    "RETENTION_DAYS": 90,
    "PRUNE_BATCH_ROWS": 5000,
    "RELAY_PRUNE": True,
    "RELAY_PRUNE_SECONDS": 60,
    "TASK_BACKEND": None,
    "AROUND_DELIVERY": None,
}


WAKE_MODES = ("notify", "poll")


def setting(key: str) -> Any:
    configured = getattr(settings, SETTINGS_NAME, {})
    return configured.get(key, DEFAULTS[key])


def get_task_backend() -> Any | None:
    """The configured task backend, or None when receivers run in the relay."""
    configured = setting("TASK_BACKEND")
    if configured is None:
        return None
    if isinstance(configured, str):
        return import_string(configured)()
    options = dict(configured)
    return import_string(options.pop("BACKEND"))(**options)


def get_codec() -> PayloadCodec:
    """Build the configured codec."""
    return import_string(setting("CODEC"))()


def get_around_delivery() -> Callable[[DeliveryContext], AbstractContextManager[Any]] | None:
    """The configured ``AROUND_DELIVERY`` factory, or None when there is none."""
    path = setting("AROUND_DELIVERY")
    return None if path is None else import_string(path)
