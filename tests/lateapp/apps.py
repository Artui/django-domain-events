"""The app configuration for the app that loads late."""

from __future__ import annotations

from django.apps import AppConfig


class LateAppConfig(AppConfig):
    name = "tests.lateapp"
    label = "lateapp"
