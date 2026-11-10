from __future__ import annotations

from typing import Any

from django.contrib import admin
from django.db.models import QuerySet
from django.http import HttpRequest

from django_domain_events.declaration.registry import registry


class EventNameFilter(admin.SimpleListFilter):
    """Filter the log by event name, from the registry rather than a SELECT DISTINCT scan."""

    title = "event"
    parameter_name = "event_name"

    def lookups(self, request: HttpRequest, model_admin: Any) -> list[tuple[str, str]]:
        return [(e.name, e.name) for e in sorted(registry.events(), key=lambda e: e.name)]

    def queryset(self, request: HttpRequest, queryset: QuerySet[Any]) -> QuerySet[Any]:
        value = self.value()
        if value is None:
            return queryset
        return queryset.filter(name=value)
