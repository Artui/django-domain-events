"""Tests mirroring ``django_domain_events/codecs/payload_codec.py``."""

from __future__ import annotations

from typing import Any

from django_domain_events.codecs.dacite_codec import DaciteCodec
from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.codecs.payload_codec import PayloadCodec


def test_both_shipped_codecs_satisfy_the_protocol() -> None:
    """The seam is only a seam if what ships through it actually fits."""

    def takes_a_codec(codec: PayloadCodec) -> Any:
        return codec

    assert takes_a_codec(DataclassCodec()) is not None
    assert takes_a_codec(DaciteCodec()) is not None


def test_the_protocol_documents_both_halves() -> None:
    assert callable(PayloadCodec.encode)
    assert callable(PayloadCodec.decode)
