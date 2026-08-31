"""Payload codecs.

``DaciteCodec`` is absent by design: it imports an optional extra, so it is
referenced by full path instead.
"""

from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.codecs.payload_codec import PayloadCodec
from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType

__all__ = ["DataclassCodec", "PayloadCodec", "UnsupportedPayloadType"]
