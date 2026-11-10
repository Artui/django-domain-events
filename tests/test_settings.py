from __future__ import annotations

from django_domain_events.codecs.dacite_codec import DaciteCodec
from django_domain_events.codecs.dataclass_codec import DataclassCodec
from django_domain_events.settings import get_codec, setting


def test_defaults_apply_when_nothing_is_configured() -> None:
    assert setting("CODEC") == "django_domain_events.codecs.dataclass_codec.DataclassCodec"
    assert isinstance(get_codec(), DataclassCodec)


def test_a_configured_codec_is_used(settings) -> None:
    """Named in settings, not sniffed: a self-picking codec would differ per machine."""
    settings.DJANGO_DOMAIN_EVENTS = {
        "CODEC": "django_domain_events.codecs.dacite_codec.DaciteCodec"
    }
    assert isinstance(get_codec(), DaciteCodec)


def test_the_codec_is_not_cached_across_a_settings_change(settings) -> None:
    """Caching at import freezes whatever was configured when the first event fired."""
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


def test_a_receivers_requested_retry_is_capped_at_a_day_by_default() -> None:
    """Its own ceiling, not BACKOFF_CAP_SECONDS, so Retry-After is not clamped."""
    assert setting("MAX_RECEIVER_RETRY_DELAY_SECONDS") == 86400.0
    assert setting("MAX_RECEIVER_RETRY_DELAY_SECONDS") != setting("BACKOFF_CAP_SECONDS")


def test_the_wake_defaults() -> None:
    """Notify on Postgres, at most one NOTIFY per half second per process."""
    assert setting("WAKE") == "notify"
    assert setting("NOTIFY_COALESCE_SECONDS") == 0.5


def test_the_relay_sweep_is_on_and_throttled_to_a_minute_by_default() -> None:
    assert setting("RELAY_PRUNE") is True
    assert setting("RELAY_PRUNE_SECONDS") == 60
