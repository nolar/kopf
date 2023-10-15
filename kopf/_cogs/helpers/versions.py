"""
Detecting the framework's own version.

The codebase does not contain the version directly, as it would require
code changes on every release. Kopf's releases depend on tagging rather
than in-code version bumps (Kopf's authour believes that versions belong
to the versioning system, not to the codebase).

The version is determined only once at startup when the code is loaded.
"""
from typing import Optional

version: Optional[str] = None

try:
    import importlib.metadata
except ImportError:
    pass
else:
    try:
        name, *_ = __name__.split('.')  # usually "kopf", unless renamed/forked.
        version = importlib.metadata.version(name)
    except Exception:
        pass  # installed as an egg, from git, etc.
