from __future__ import annotations

from typing import Any

from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest

from django_domain_events.admin.receiver_key_filter import ReceiverKeyFilter
from django_domain_events.admin.utils import may_change
from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.operations.cancel_deliveries import cancel_deliveries
from django_domain_events.operations.requeue_dead import requeue_dead


@admin.register(DeliveryRecord)
class DeliveryRecordAdmin(admin.ModelAdmin):
    """The dead-letter queue: browsable, with requeue and cancel as actions."""

    list_display = (
        "receiver_key",
        "target",
        "event",
        "status",
        "attempts",
        "available_at",
        "claimed_by",
    )
    list_filter = ("status", ReceiverKeyFilter)
    search_fields = ("receiver_key", "target", "last_error")
    ordering = ("-pk",)
    # No date hierarchy: it scans the whole table on every page load.
    show_full_result_count = False
    actions = ("requeue", "cancel")
    list_select_related = ("event",)

    @admin.action(description="Requeue selected dead deliveries", permissions=["requeue"])
    def requeue(self, request: HttpRequest, queryset: QuerySet[DeliveryRecord]) -> None:
        # Through requeue_dead: a plain update would leave rows that dead-letter again.
        ids = list(queryset.values_list("pk", flat=True))
        count = requeue_dead(delivery_ids=ids)
        skipped = len(ids) - count
        note = f" {skipped} were not dead and were left alone." if skipped else ""
        self.message_user(request, f"Requeued {count} deliveries.{note}", messages.SUCCESS)

    def has_requeue_permission(self, request: HttpRequest) -> bool:
        """Who may requeue; see ``may_change`` for why an action needs its own predicate."""
        return may_change(request, self.opts)

    @admin.action(description="Cancel selected deliveries", permissions=["cancel"])
    def cancel(self, request: HttpRequest, queryset: QuerySet[DeliveryRecord]) -> None:
        # Through cancel_deliveries: it locks the events as replay does, and checks status.
        rows = list(queryset.values_list("pk", "event"))
        counts = cancel_deliveries(
            [event for _, event in rows], delivery_ids=[pk for pk, _ in rows]
        )
        settled = len(rows) - counts["cancelled"] - counts["in_flight"]
        note = ""
        if counts["in_flight"]:
            note += f" {counts['in_flight']} were in flight and were left to run."
        if settled:
            note += f" {settled} were already settled and were left alone."
        self.message_user(
            request, f"Cancelled {counts['cancelled']} deliveries.{note}", messages.SUCCESS
        )

    def has_cancel_permission(self, request: HttpRequest) -> bool:
        """Who may cancel; see ``may_change`` for why an action needs its own predicate."""
        return may_change(request, self.opts)

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def get_readonly_fields(self, request: HttpRequest, obj: Any = None) -> tuple[str, ...]:
        # Shown as hex: the raw bytes would print as a Python bytes literal.
        return tuple(
            "digest" if field.name == "target_digest" else field.name
            for field in self.model._meta.fields
        )

    @admin.display(description="target digest")
    def digest(self, obj: DeliveryRecord) -> str:
        """SHA-256 of the target, in hex: the form a digest is compared in."""
        return obj.target_digest.hex()
