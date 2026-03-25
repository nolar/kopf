import importlib.metadata
import re

import aiohttp


def _parse_version(version_string: str) -> tuple[int, ...]:
    parts: list[int] = []
    for part in version_string.split('.')[:3]:
        m = re.match(r'\d+', part)
        if m is None:
            raise ValueError(f"Cannot parse version part: {part!r}")
        parts.append(int(m.group()))
    return tuple(parts)


def _check_aiohttp_has_graceful_shutdown() -> bool:
    try:
        version = _parse_version(aiohttp.__version__)
        return version >= (3, 12, 4)
    except (AttributeError, ValueError):  # if aiohttp.__version__ is gone
        pass
    try:
        version = _parse_version(importlib.metadata.version('aiohttp'))
        return version >= (3, 12, 4)
    except (AttributeError, ValueError):
        pass
    return True  # optimistically assume it works


AIOHTTP_HAS_GRACEFUL_SHUTDOWN = _check_aiohttp_has_graceful_shutdown()
