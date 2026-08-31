"""App configuration: autodiscovery and check registration."""

from __future__ import annotations

from django.apps import AppConfig
from django.core.checks import Tags, register
from django.utils.module_loading import autodiscover_modules


class DjangoDomainEventsConfig(AppConfig):
    """Wires the registry up at startup.

    Autodiscovery of an ``events`` module in each installed app, the same pattern
    ``django.contrib.admin`` uses for ``admin``. It runs in ``ready()``, which
    matters beyond convenience: the default event and receiver names are derived
    from the app label, and ``apps.get_containing_app_config`` only answers once
    the registry is populated. Importing declarations any earlier makes that
    lookup fail for reasons that read like a bug in this package.
    """

    name = "django_domain_events"
    verbose_name = "Domain events"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from django_domain_events import checks

        register(checks.check_receivers_have_events)
        register(checks.check_codec_dependency_is_installed)
        register(checks.check_no_orphaned_deliveries, Tags.database)

        autodiscover_modules("events")
