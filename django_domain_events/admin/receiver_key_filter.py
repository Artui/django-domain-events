from __future__ import annotations

from typing import Any

from django.contrib import admin
from django.db.models import QuerySet
from django.http import HttpRequest

from django_domain_events.registry import registry


class ReceiverKeyFilter(admin.SimpleListFilter):
    """Filter deliveries by receiver key, from the registry.

    Same reasoning as EventNameFilter, and the same bonus: a receiver that has
    never been delivered anything is listed, which is the one an operator is
    looking for.
    """

    title = "receiver"
    parameter_name = "receiver"

    def lookups(self, request: HttpRequest, model_admin: Any) -> list[tuple[str, str]]:
        return [(r.key, r.key) for r in sorted(registry.receivers(), key=lambda r: r.key)]

    def queryset(self, request: HttpRequest, queryset: QuerySet[Any]) -> QuerySet[Any]:
        value = self.value()
        if value is None:
            return queryset
        return queryset.filter(receiver_key=value)
