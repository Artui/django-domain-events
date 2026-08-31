from __future__ import annotations

from typing import Any

from django.contrib import admin
from django.db.models import QuerySet
from django.http import HttpRequest

from django_domain_events.registry import registry


class EventNameFilter(admin.SimpleListFilter):
    """Filter the log by event name, using the registry rather than the table.

    Django's field filter builds its options with ``SELECT DISTINCT name`` over
    the whole log on every changelist load, which is a full scan of the table
    this package tells operators will be the largest in their database.

    The registry knows the same answer for free, and knows it better: an event
    declared but never fired appears in the list, and selecting it shows the
    empty result that is itself the finding.
    """

    title = "event"
    parameter_name = "event_name"

    def lookups(self, request: HttpRequest, model_admin: Any) -> list[tuple[str, str]]:
        return [(e.name, e.name) for e in sorted(registry.events(), key=lambda e: e.name)]

    def queryset(self, request: HttpRequest, queryset: QuerySet[Any]) -> QuerySet[Any]:
        value = self.value()
        if value is None:
            return queryset
        return queryset.filter(name=value)
