"""
The main Kopf module for all the exported functions & classes.
"""

# Unlike all other places, where we import other modules and refer
# the functions via the modules, this is the framework's top-level interface,
# as it is seen by the users. So, we export the individual functions.

from kopf import (
    on,  # as a separate name on the public namespace
)
from kopf.clients.auth import (
    login,
    LoginError,
)
from kopf.config import (
    configure,
    LOGLEVEL_INFO,
    LOGLEVEL_WARNING,
    LOGLEVEL_ERROR,
    LOGLEVEL_CRITICAL,
    EventsConfig,
    WorkersConfig
)
from kopf.engines.posting import (
    event,
    info,
    warn,
    exception,
)
from kopf.on import (
    register,
)
from kopf.reactor import (
    lifecycles,  # as a separate name on the public namespace
)
from kopf.reactor.handling import (
    HandlerRetryError,
    HandlerFatalError,
    HandlerTimeoutError,
    execute,
)
from kopf.reactor.lifecycles import (
    get_default_lifecycle,
    set_default_lifecycle,
)
from kopf.reactor.queueing import (
    run,
    create_tasks,
)
from kopf.reactor.registries import (
    BaseRegistry,
    SimpleRegistry,
    GlobalRegistry,
    get_default_registry,
    set_default_registry,
)
from kopf.structs.hierarchies import (
    adopt,
    label,
    harmonize_naming,
    adjust_namespace,
    build_object_reference,
    build_owner_reference,
    append_owner_reference,
    remove_owner_reference,
)

__all__ = [
    'on', 'lifecycles', 'register', 'execute',
    'configure',
    'login', 'LoginError',
    'event', 'info', 'warn', 'exception',
    'run', 'create_tasks',
    'adopt', 'label',
    'get_default_lifecycle', 'set_default_lifecycle',
    'build_object_reference', 'build_owner_reference',
    'append_owner_reference', 'remove_owner_reference',
    'HandlerRetryError',
    'HandlerFatalError',
    'HandlerTimeoutError',
    'BaseRegistry',
    'SimpleRegistry',
    'GlobalRegistry',
    'get_default_registry',
    'set_default_registry',
]
