"""App configuration: autodiscovery and check registration."""

from __future__ import annotations

from django.apps import AppConfig
from django.core.checks import Tags, register
from django.utils.module_loading import autodiscover_modules


class DjangoDomainEventsConfig(AppConfig):
    """Wires the registry up at startup.

    Autodiscovery runs in ``ready()`` because default names derive from the app
    label, and ``get_containing_app_config`` only answers once the app registry
    is populated.
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
