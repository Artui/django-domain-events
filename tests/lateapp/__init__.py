"""An app the test settings do not install.

It exists to be installed partway through one test, after a wildcard receiver
has already been declared, so that an event is declared by an app that genuinely
loads later - through Django's own app loading and the substrate's own
autodiscovery, not by registering a class by hand.
"""
