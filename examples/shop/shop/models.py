from __future__ import annotations

from django.conf import settings
from django.db import models


class Order(models.Model):
    customer = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    sku = models.CharField(max_length=32)
    quantity = models.PositiveIntegerField()
    total_cents = models.PositiveIntegerField()
    cancelled = models.BooleanField(default=False)


class StockLevel(models.Model):
    sku = models.CharField(max_length=32, unique=True)
    available = models.IntegerField()


class Reservation(models.Model):
    order = models.OneToOneField(Order, on_delete=models.CASCADE)
    quantity = models.PositiveIntegerField()


class SentEmail(models.Model):
    """Stands in for a side effect the database cannot roll back."""

    to = models.CharField(max_length=254)
    subject = models.CharField(max_length=200)
    sent_at = models.DateTimeField(auto_now_add=True)


class PartnerSubscription(models.Model):
    """A partner system that wants one event. Rows an operator edits, not code.

    Which is the whole reason the forwarding receiver names its destinations
    with `targets=` rather than being declared once per partner: the set lives
    in data and changes without a deploy.
    """

    partner = models.CharField(max_length=64)
    event_name = models.CharField(max_length=255, db_index=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["partner", "event_name"], name="one_subscription_each")
        ]


class PartnerNotice(models.Model):
    """Stands in for the request a partner would be sent."""

    partner = models.CharField(max_length=64)
    event_name = models.CharField(max_length=255)
    event_id = models.BigIntegerField()
    sent_at = models.DateTimeField(auto_now_add=True)
