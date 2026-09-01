from __future__ import annotations

from typing import Any

from django.contrib.auth import get_permission_codename
from django.http import HttpRequest


def may_change(request: HttpRequest, opts: Any) -> bool:
    """Whether this user holds the model's change permission.

    Both admin actions gate on it. Django offers an action with no
    ``permissions=`` to anyone who can reach the changelist, which is view-only
    staff - and ``has_change_permission`` gates the form alone, so refusing
    there does not refuse the action.

    The change permission rather than a custom one: it means "may mutate these
    rows", which is what a replay or a requeue does, and it needs no migration.
    ``get_permission_codename`` rather than an f-string so a model renaming its
    permissions is still asked about the right one.

    ``request.user`` is typed as possibly anonymous and ``has_perm`` lives on
    ``PermissionsMixin``, which a swapped user model need not have. Django's own
    ``ModelAdmin`` calls it unconditionally here; the ``Any`` records that this
    is that boundary.
    """
    user: Any = request.user
    return user.has_perm(f"{opts.app_label}.{get_permission_codename('change', opts)}")
