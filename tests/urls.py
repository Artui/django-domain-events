"""URLconf for the suite.

Only the admin, and only because ``django.contrib.admin``'s system checks want
a URLconf to exist. Nothing under test routes through it.
"""

from __future__ import annotations

from django.contrib import admin
from django.urls import path

urlpatterns = [path("admin/", admin.site.urls)]
