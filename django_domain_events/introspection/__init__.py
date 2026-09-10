"""What is declared, what listens to it, and whether it is healthy.

Deliberately no re-exports. These are internal groupings, not public namespaces:
the package root is the public surface and re-exports everything, and every
module inside the package imports its neighbours by leaf path already.

Re-exporting here would buy nothing and cost a class of circular import. A leaf
import runs the parent package first, so an eager __init__ makes
``django_domain_events.declaration.registry`` pull in ``event``, which imports
``utils``, which imports ``registry``. This package was bitten by the same shape
before -- ``payload_upgrade_failed`` sits at the root for exactly that reason,
and says so.
"""
