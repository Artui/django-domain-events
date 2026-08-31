from __future__ import annotations

from typing import Any

from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest
from django.utils.timesince import timesince

from django_domain_events.models.event_record import EventRecord
from django_domain_events.replay_events import replay_events
from django_domain_events.types.delivery_status import DeliveryStatus


@admin.register(EventRecord)
class EventRecordAdmin(admin.ModelAdmin):
    """Read-only browsing of the log, plus replay.

    Every field is read-only and nothing can be added by hand. An event log
    whose rows can be edited in a form is not evidence of anything, and the one
    guarantee this package sells is that a row exists if and only if the change
    committed.
    """

    list_display = ("name", "version", "actor_label", "owed", "suppressed_reason", "age")
    list_filter = ("name", "version", "recorded_at")
    search_fields = ("name", "actor_key", "actor_label", "dedupe_key")
    date_hierarchy = "recorded_at"
    ordering = ("-pk",)
    actions = ("replay",)

    def get_queryset(self, request: HttpRequest) -> QuerySet[EventRecord]:
        return super().get_queryset(request).prefetch_related("deliveries")

    @admin.display(description="deliveries owed")
    def owed(self, obj: EventRecord) -> int:
        settled = (DeliveryStatus.SUCCEEDED, DeliveryStatus.DEAD, DeliveryStatus.ORPHANED)
        # Counted in Python over the prefetch rather than with a second query
        # per row: the changelist renders a hundred of these.
        return sum(1 for d in obj.deliveries.all() if d.status not in settled)

    @admin.display(description="age")
    def age(self, obj: EventRecord) -> str:
        return timesince(obj.recorded_at)

    @admin.action(description="Replay selected events")
    def replay(self, request: HttpRequest, queryset: QuerySet[EventRecord]) -> None:
        counts = replay_events(queryset.values_list("pk", flat=True))
        self.message_user(
            request,
            f"Reopened {counts['reopened']} deliveries and added {counts['added']}.",
            messages.SUCCESS,
        )

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        # Deleting here would cascade owed deliveries away with no record
        # that anything was lost. prune_events is the supported route: it
        # re-checks settledness at the delete, which a form cannot.
        return False

    def get_readonly_fields(self, request: HttpRequest, obj: Any = None) -> tuple[str, ...]:
        return tuple(field.name for field in self.model._meta.fields)
