from __future__ import annotations

from io import StringIO

import pytest
from django.core.management import call_command

pytestmark = pytest.mark.django_db(transaction=True)


def test_adding_the_cancelled_status_changes_no_schema() -> None:
    """A choices-only change: Django records it, and no backend gets a statement
    from it, so a delivery table of any size takes it without a rewrite or a
    lock. Run on both backends the suite targets."""
    out = StringIO()
    call_command("sqlmigrate", "django_domain_events", "0008", stdout=out)

    statements = [
        line
        for line in out.getvalue().splitlines()
        if line.strip() and not line.startswith("--") and line not in ("BEGIN;", "COMMIT;")
    ]
    assert statements == []
    assert "(no-op)" in out.getvalue()
