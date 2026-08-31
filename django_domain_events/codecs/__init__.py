"""Payload codecs.

``DaciteCodec`` is deliberately absent from these re-exports. It imports
``dacite``, which is an optional extra, and re-exporting it here would import
that dependency for anyone who touches this package at all. It is referenced by
its full path instead: ``django_domain_events.codecs.dacite_codec.DaciteCodec``.
"""

from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.codecs.payload_codec import PayloadCodec
from django_domain_events.codecs.unsupported_payload_type import UnsupportedPayloadType

__all__ = ["DataclassCodec", "PayloadCodec", "UnsupportedPayloadType"]
