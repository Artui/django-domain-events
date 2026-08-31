from __future__ import annotations

from datetime import datetime, time
from enum import Enum
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder


class PayloadEncoder(DjangoJSONEncoder):
    """Writes exactly what the decode side claims to rebuild.

    ``DjangoJSONEncoder`` is close but not the same table, and both differences
    are silent:

    - It has no ``Enum`` branch, so a plain ``Enum`` raised ``TypeError`` from
      inside the caller's transaction while the decoder advertised enums as
      supported. Only a ``str``-mixin enum survived, which is what the tests
      happened to use.
    - It truncates ``datetime`` and ``time`` to milliseconds, so an event
      round-tripped back with different data and nothing failed.
    """

    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, (datetime, time)):
            return o.isoformat()
        return super().default(o)
