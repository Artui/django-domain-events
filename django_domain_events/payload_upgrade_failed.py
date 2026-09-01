from __future__ import annotations


class PayloadUpgradeFailed(Exception):
    """An event class's ``upgrade`` hook raised while migrating an old row.

    Its own type because an operator reading ``last_error`` needs to know the
    hook ran and what it said, not that something unspecified went wrong
    somewhere between the row and the receiver.

    At the package root rather than under ``codecs/``: it is raised before the
    codec sees the payload, and importing it from there pulls in
    ``codecs/__init__``, which imports the module that imports this one.
    """
