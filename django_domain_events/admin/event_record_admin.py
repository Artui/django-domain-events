from __future__ import annotations

from typing import Any

from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest
from django.utils.timesince import timesince

from django_domain_events.admin.event_name_filter import EventNameFilter
from django_domain_events.admin.utils import may_change
from django_domain_events.models.event_record import EventRecord
from django_domain_events.operations.cancel_deliveries import cancel_deliveries
from django_domain_events.operations.replay_events import replay_events
from django_domain_events.utils import TERMINAL


@admin.register(EventRecord)
class EventRecordAdmin(admin.ModelAdmin):
    """Read-only browsing of the log, plus replay and cancel."""

    list_display = ("name", "version", "actor_label", "owed", "suppressed_reason", "age")
    list_filter = (EventNameFilter,)
    search_fields = ("name", "actor_key", "actor_label", "dedupe_key")
    ordering = ("-pk",)
    # Both off: each would scan the whole log on every page load.
    show_full_result_count = False
    actions = ("replay", "cancel")

    def get_queryset(self, request: HttpRequest) -> QuerySet[EventRecord]:
        return super().get_queryset(request).prefetch_related("deliveries")

    @admin.display(description="deliveries owed")
    def owed(self, obj: EventRecord) -> int:
        # Counted over the prefetch, so the changelist does not query once per row.
        return sum(1 for d in obj.deliveries.all() if d.status not in TERMINAL)

    @admin.display(description="age")
    def age(self, obj: EventRecord) -> str:
        return timesince(obj.recorded_at)

    @admin.action(description="Replay selected events", permissions=["replay"])
    def replay(self, request: HttpRequest, queryset: QuerySet[EventRecord]) -> None:
        counts = replay_events(queryset.values_list("pk", flat=True))
        self.message_user(
            request,
            f"Reopened {counts['reopened']} deliveries and added {counts['added']}.",
            messages.SUCCESS,
        )

    def has_replay_permission(self, request: HttpRequest) -> bool:
        """Who may replay; see ``may_change`` for why an action needs its own predicate."""
        return may_change(request, self.opts)

    @admin.action(description="Cancel owed deliveries of selected events", permissions=["cancel"])
    def cancel(self, request: HttpRequest, queryset: QuerySet[EventRecord]) -> None:
        # Every receiver's owed rows of these events, through cancel_deliveries.
        counts = cancel_deliveries(queryset.values_list("pk", flat=True))
        self.message_user(
            request,
            f"Cancelled {counts['cancelled']} deliveries and left "
            f"{counts['in_flight']} in flight to run.",
            messages.SUCCESS,
        )

    def has_cancel_permission(self, request: HttpRequest) -> bool:
        """Who may cancel; see ``may_change`` for why an action needs its own predicate."""
        return may_change(request, self.opts)

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        # Would cascade owed deliveries away silently: prune_events re-checks settledness.
        return False

    def get_readonly_fields(self, request: HttpRequest, obj: Any = None) -> tuple[str, ...]:
        return tuple(field.name for field in self.model._meta.fields)
