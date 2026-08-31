"""A Django domain-event log with in-process fan-out.

The re-exports are eager, so the modules reached from here must not import a
model at module level: Django imports an app's package before the app registry
is ready. The four that touch models import them inside their functions.

A lazy PEP 562 re-export does not work here. Under one-symbol-per-file the
module and the symbol share a name, so importing the submodule binds the module
as an attribute of this package and ``from django_domain_events import fire``
then hands back a module.
"""

from django_domain_events.assert_fired import assert_fired
from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.codecs.payload_codec import PayloadCodec
from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType
from django_domain_events.deliver import deliver_one, deliver_pending
from django_domain_events.drain_outbox import drain_outbox
from django_domain_events.event import event
from django_domain_events.fire import fire
from django_domain_events.receiver import receiver
from django_domain_events.registry import Registry, registry
from django_domain_events.types.delivery_context import DeliveryContext
from django_domain_events.types.delivery_mode import DeliveryMode
from django_domain_events.types.delivery_status import DeliveryStatus
from django_domain_events.version import __version__

DURABLE = DeliveryMode.DURABLE
INLINE = DeliveryMode.INLINE
ON_COMMIT = DeliveryMode.ON_COMMIT

__all__ = [
    "DURABLE",
    "INLINE",
    "ON_COMMIT",
    "DataclassCodec",
    "DeliveryContext",
    "DeliveryMode",
    "DeliveryStatus",
    "PayloadCodec",
    "Registry",
    "UnsupportedPayloadType",
    "__version__",
    "assert_fired",
    "deliver_one",
    "deliver_pending",
    "drain_outbox",
    "event",
    "fire",
    "receiver",
    "registry",
]
