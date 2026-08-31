"""A minimal installed app, so declarations have an app label to derive from."""

from django.apps import AppConfig


class TestAppConfig(AppConfig):
    name = "tests.testapp"
    label = "testapp"
