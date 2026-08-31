from __future__ import annotations

from typing import Any

from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest

from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.requeue_dead import requeue_dead


@admin.register(DeliveryRecord)
class DeliveryRecordAdmin(admin.ModelAdmin):
    """The dead-letter queue, browsable, with requeue as an action.

    Read-only for the same reason the event log is: editing ``status`` by hand
    is how a claimed row gets handed to a second worker.
    """

    list_display = ("receiver_key", "event", "status", "attempts", "available_at", "claimed_by")
    list_filter = ("status", "receiver_key")
    search_fields = ("receiver_key", "last_error")
    date_hierarchy = "available_at"
    ordering = ("-pk",)
    actions = ("requeue",)
    list_select_related = ("event",)

    @admin.action(description="Requeue selected dead deliveries")
    def requeue(self, request: HttpRequest, queryset: QuerySet[DeliveryRecord]) -> None:
        # Through requeue_dead rather than queryset.update(): it resets the
        # attempt budget, clears the lease and the stale error, and wakes the
        # relay. An action that only flipped status would leave rows that
        # dead-letter again on the first failure.
        # Both counts come from one list captured before the update. Asking
        # the queryset afterwards would re-run it against rows this call just
        # moved to PENDING, and report every requeued row as skipped.
        ids = list(queryset.values_list("pk", flat=True))
        count = requeue_dead(delivery_ids=ids)
        skipped = len(ids) - count
        note = f" {skipped} were not dead and were left alone." if skipped else ""
        self.message_user(request, f"Requeued {count} deliveries.{note}", messages.SUCCESS)

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def get_readonly_fields(self, request: HttpRequest, obj: Any = None) -> tuple[str, ...]:
        return tuple(field.name for field in self.model._meta.fields)
