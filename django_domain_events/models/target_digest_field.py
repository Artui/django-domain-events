from __future__ import annotations

from typing import Any

from django.db import models

from django_domain_events.utils import target_digest


class TargetDigestField(models.BinaryField):
    """A delivery row's target digest, derived from its target whenever it is written.

    Never set by hand, and deliberately without a default. Django calls a
    field's ``pre_save`` for every object an insert writes - ``bulk_create``
    included, which is the same hook ``auto_now`` relies on - and for every
    ``save()``, so the digest is computed from the row's own ``target`` at the
    moment the row reaches the database, whichever way it got there. A default
    equal to the blank target's digest would instead let a write that set a
    target and no digest store the wrong one without complaint, and a write
    that supplies a digest of its own is overwritten rather than trusted.

    The one write it cannot see is ``QuerySet.update(target=...)``, which runs
    no field code at all. Nothing in this package changes a target after the
    row is written; a target is part of what a delivery *is*.

    The 32 raw bytes of a SHA-256, so the unique constraint indexes a short
    value however long the target is. Binary fields are not editable, which is
    also what keeps this one out of every form.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["max_length"] = 32
        super().__init__(*args, **kwargs)

    def deconstruct(self) -> Any:
        # BinaryField's own deconstruct already drops ``editable`` when it is
        # the default False, so only the fixed width is ours to remove.
        name, path, args, kwargs = super().deconstruct()
        del kwargs["max_length"]
        return name, path, args, kwargs

    def db_type(self, connection: Any) -> str | None:
        # MySQL maps a BinaryField to ``longblob``, and refuses a blob in a
        # unique index without a prefix length; a digest is a fixed width, so
        # it gets the bounded type instead. Only a stub connection tests this
        # (test_mysql_gets_a_bounded_binary_column_it_can_index): the suite
        # runs on SQLite and Postgres, and no real MySQL server has seen it.
        if connection.vendor == "mysql":
            return "varbinary(32)"
        return super().db_type(connection)

    def pre_save(self, model_instance: Any, add: bool) -> bytes:
        digest = target_digest(model_instance.target)
        setattr(model_instance, self.attname, digest)
        return digest
