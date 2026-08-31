"""Django discovers models through this module, so it is the re-export point."""

from django_domain_events.models.delivery_record import DeliveryRecord
from django_domain_events.models.event_record import EventRecord

__all__ = ["DeliveryRecord", "EventRecord"]
