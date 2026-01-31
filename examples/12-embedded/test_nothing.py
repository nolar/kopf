"""
Embeddable operators require very customised application-specific testing.
Kopf cannot help here beyond its regular :class:`kopf.testing.KopfRunner`,
which is an equivalent of the ``kopf run`` command.

This file exists to disable the implicit e2e tests
(they skip if explicit e2e tests exist in the example directory).
"""
