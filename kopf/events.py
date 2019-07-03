"""
**THIS MODULE IS DEPRECATED AND WILL BE REMOVED.**
"""
import warnings

from kopf.engines.posting import (
    event,
    info,
    warn,
    exception,
)

__all__ = ['event', 'info', 'warn', 'exception']


# Triggered on explicit `import kopf.events` (not imported this way normally).
warnings.warn(
    "`kopf.events` is deprecated; "
    "use `kopf` directly: e.g. `kopf.event(...)`.",
    DeprecationWarning, stacklevel=0)
