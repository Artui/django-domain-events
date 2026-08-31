"""Tests mirroring ``django_domain_events/settings.py``."""

from __future__ import annotations

from django_domain_events.codecs.dacite_codec import DaciteCodec
from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.settings import get_codec, setting


def test_defaults_apply_when_nothing_is_configured() -> None:
    assert setting("CODEC") == "django_domain_events.codecs.dataclass_codec.DataclassCodec"
    assert isinstance(get_codec(), DataclassCodec)


def test_a_configured_codec_is_used(settings) -> None:
    """Named in settings rather than sniffed from what happens to be installed:
    a codec that picks itself means the same event decodes on one machine and
    raises on another, silently."""
    settings.DJANGO_DOMAIN_EVENTS = {
        "CODEC": "django_domain_events.codecs.dacite_codec.DaciteCodec"
    }
    assert isinstance(get_codec(), DaciteCodec)


def test_the_codec_is_not_cached_across_a_settings_change(settings) -> None:
    """Caching it at import would freeze whatever was configured when the first
    event fired, which makes a test pass or fail on what ran before it."""
    assert isinstance(get_codec(), DataclassCodec)
    settings.DJANGO_DOMAIN_EVENTS = {
        "CODEC": "django_domain_events.codecs.dacite_codec.DaciteCodec"
    }
    assert isinstance(get_codec(), DaciteCodec)


def test_partial_configuration_keeps_the_other_defaults(settings) -> None:
    settings.DJANGO_DOMAIN_EVENTS = {
        "CODEC": "django_domain_events.codecs.dacite_codec.DaciteCodec"
    }
    assert setting("WARN_OUTSIDE_ATOMIC") is True
