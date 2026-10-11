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
    """The dead-letter queue, browsable, with requeue and cancel as actions.

    Read-only for the same reason the event log is: editing ``status`` by hand
    is how a claimed row gets handed to a second worker.
    """

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
    # The target is searchable because it is what an operator is holding when a
    # fan-out delivery goes wrong: "endpoint 42 says it never got it".
    search_fields = ("receiver_key", "target", "last_error")
    ordering = ("-pk",)
    # No date hierarchy on available_at: the model keeps no plain index on
    # it on purpose, and a hierarchy would put MIN/MAX and a SELECT DISTINCT
    # date_trunc over that column on every page load.
    show_full_result_count = False
    actions = ("requeue", "cancel")
    list_select_related = ("event",)

    @admin.action(description="Requeue selected dead deliveries", permissions=["requeue"])
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

    def has_requeue_permission(self, request: HttpRequest) -> bool:
        """Who may requeue, checked separately from who may open the changelist.

        See ``may_change`` for why an action needs its own predicate.
        """
        return may_change(request, self.opts)

    @admin.action(description="Cancel selected deliveries", permissions=["cancel"])
    def cancel(self, request: HttpRequest, queryset: QuerySet[DeliveryRecord]) -> None:
        # Through cancel_deliveries rather than queryset.update(): it locks the
        # events as replay does, so a cancel cannot be overtaken by a replay
        # open on the same event, and it conditions the write on the status,
        # so a row a worker claimed after the page loaded is left to run.
        # The selected rows are passed as delivery_ids and their own events as
        # the scope, so the selection is exactly what is withdrawn: a sibling
        # receiver's row of the same event is not
        # (test_cancel_touches_only_the_selected_rows_of_an_event).
        # One list captured before the call, as requeue captures it, so the
        # settled count is not taken from rows this call just settled.
        rows = list(queryset.values_list("pk", "event"))
        counts = cancel_deliveries(
            [event for _, event in rows], delivery_ids=[pk for pk, _ in rows]
        )
        settled = len(rows) - counts["cancelled"] - counts["in_flight"]
        # Two notes, two separate arcs, each with a test asserting the exact
        # message: test_cancel_reports_a_claimed_row_as_in_flight and
        # test_cancel_reports_settled_rows_it_left_alone.
        note = ""
        if counts["in_flight"]:
            note += f" {counts['in_flight']} were in flight and were left to run."
        if settled:
            note += f" {settled} were already settled and were left alone."
        self.message_user(
            request, f"Cancelled {counts['cancelled']} deliveries.{note}", messages.SUCCESS
        )

    def has_cancel_permission(self, request: HttpRequest) -> bool:
        """Who may cancel, checked separately from who may open the changelist.

        See ``may_change`` for why an action needs its own predicate.
        """
        return may_change(request, self.opts)

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def get_readonly_fields(self, request: HttpRequest, obj: Any = None) -> tuple[str, ...]:
        # The digest is swapped for its hex rendering: stored as raw bytes,
        # which the admin would print as a Python bytes literal.
        return tuple(
            "digest" if field.name == "target_digest" else field.name
            for field in self.model._meta.fields
        )

    @admin.display(description="target digest")
    def digest(self, obj: DeliveryRecord) -> str:
        """SHA-256 of the target in hex, the form a digest computed anywhere
        else is compared in."""
        return obj.target_digest.hex()
