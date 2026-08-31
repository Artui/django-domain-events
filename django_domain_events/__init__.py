"""A Django domain-event log with in-process fan-out.

The public surface is re-exported eagerly, which takes some arranging: Django
imports an app's package *before* the app registry is ready, so anything reached
from here must not import a model at module level. The four modules that touch
models therefore import them inside their functions, each saying so inline. That
is this package's one standing exception to the top-level-imports rule, and it
buys something worth the cost.

A lazy PEP 562 re-export was the obvious alternative and it does not work here.
Under one-symbol-per-file the module and the symbol share a name -- ``fire``
lives in ``fire.py`` -- and importing that submodule makes the import system bind
the *module* as an attribute of this package. From then on ordinary lookup
succeeds with the module and ``__getattr__`` is never consulted, so
``from django_domain_events import fire`` hands back a module. Caching the
resolved value only wins if nothing imports the submodule afterwards, and this
package's own internals do exactly that.
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
