from __future__ import annotations

from typing import Any

from django.db import models

from django_domain_events.utils import target_digest


class TargetDigestField(models.BinaryField):
    """A delivery row's target digest, derived from its target whenever it is written."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["max_length"] = 32
        super().__init__(*args, **kwargs)

    def deconstruct(self) -> Any:
        # BinaryField already drops editable at its default, so only the fixed width is ours.
        name, path, args, kwargs = super().deconstruct()
        del kwargs["max_length"]
        return name, path, args, kwargs

    def db_type(self, connection: Any) -> str | None:
        # MySQL refuses a blob in a unique index, and a digest is fixed width: the bounded type.
        if connection.vendor == "mysql":
            return "varbinary(32)"
        return super().db_type(connection)

    # No placeholder hook, so Postgres can unnest a bulk insert of digests.
    @property
    def get_placeholder(self) -> Any:
        raise AttributeError("get_placeholder")

    @property
    def get_placeholder_sql(self) -> Any:
        raise AttributeError("get_placeholder_sql")

    def pre_save(self, model_instance: Any, add: bool) -> bytes:
        digest = target_digest(model_instance.target)
        setattr(model_instance, self.attname, digest)
        return digest
