"""
Rudimentary type [re-]definitions for cross-versioned Python & mypy.
"""
import logging
from typing import Any, TypeAlias

# As publicly exposed: we only promise that it is based on one of the built-in loggable classes.
# Mind that these classes have multi-versioned stubs, so we avoid redefining the protocol ourselves.
Logger: TypeAlias = logging.Logger | logging.LoggerAdapter[Any]
