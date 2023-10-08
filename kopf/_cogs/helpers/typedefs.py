"""
Rudimentary type [re-]definitions for cross-versioned Python & mypy.

The problem is that new mypy versions often bring type-sheds with StdLib types
defined as generics, while the old Python runtime (down to 3.8 & 3.9 & 3.10)
does not support the usual syntax.
Examples: asyncio.Task, asyncio.Future, logging.LoggerAdapter, and others.

This modules defines them in a most suitable and reusable way. Plus it adds
some common plain type definitions used across the codebase (for convenience).
"""
import logging
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    LoggerAdapter = logging.LoggerAdapter[Any]
else:
    LoggerAdapter = logging.LoggerAdapter

# As publicly exposed: we only promise that it is based on one of the built-in loggable classes.
# Mind that these classes have multi-versioned stubs, so we avoid redefining the protocol ourselves.
Logger = Union[logging.Logger, LoggerAdapter]
