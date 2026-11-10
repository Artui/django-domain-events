from __future__ import annotations

from datetime import datetime, time
from enum import Enum
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder


class PayloadEncoder(DjangoJSONEncoder):
    """Writes exactly what the decoder rebuilds; DjangoJSONEncoder differs silently."""

    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, (datetime, time)):
            return o.isoformat()
        return super().default(o)
