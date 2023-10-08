"""
The main Kopf module for all the exported functions & classes.
"""
# isort: skip_file

# Unlike all other places, where we import other modules and refer
# the functions via the modules, this is the framework's top-level interface,
# as it is seen by the users. So, we export the individual functions.

from kopf import (
    on,  # as a separate name on the public namespace
)
from kopf.on import (
    subhandler,
    register,
    daemon,
    timer,
    index,
)
from kopf._cogs.configs.configuration import (
    OperatorSettings,
)
from kopf._cogs.configs.diffbase import (
    DiffBaseStorage,
    AnnotationsDiffBaseStorage,
    StatusDiffBaseStorage,
    MultiDiffBaseStorage,
)
from kopf._cogs.configs.progress import (
    ProgressRecord,
    ProgressStorage,
    AnnotationsProgressStorage,
    StatusProgressStorage,
    MultiProgressStorage,
    SmartProgressStorage,
)
from kopf._cogs.helpers.typedefs import (
    Logger,
)
from kopf._cogs.helpers.versions import (
    version as __version__,
)
from kopf._cogs.structs.bodies import (
    RawEventType,
    RawEvent,
    RawBody,
    Status,
    Spec,
    Meta,
    Body,
    BodyEssence,
    Labels,
    Annotations,
    OwnerReference,
    ObjectReference,
    build_object_reference,
    build_owner_reference,
)
from kopf._cogs.structs.credentials import (
    LoginError,
    ConnectionInfo,
)
from kopf._cogs.structs.dicts import (
    FieldSpec,
    FieldPath,
)
from kopf._cogs.structs.diffs import (
    Diff,
    DiffItem,
    DiffOperation,
)
from kopf._cogs.structs.ephemera import (
    Memo,
    Index,
    Store,
)
from kopf._cogs.structs.ids import (
    HandlerId,
)
from kopf._cogs.structs.patches import (
    Patch,
)
from kopf._cogs.structs.references import (
    Resource,
    EVERYTHING,
)
from kopf._cogs.structs.reviews import (
    WebhookClientConfigService,
    WebhookClientConfig,
    Operation,
    UserInfo,
    Headers,
    SSLPeer,
    WebhookFn,
    WebhookServerProtocol,
)
from kopf._core.actions import (
    lifecycles,  # as a separate name on the public namespace
)
from kopf._core.actions.execution import (
    ErrorsMode,
    TemporaryError,
    PermanentError,
    HandlerTimeoutError,
    HandlerRetriesError,
)
from kopf._core.actions.lifecycles import (
    get_default_lifecycle,
    set_default_lifecycle,
)
from kopf._core.actions.loggers import (
    configure,
    LogFormat,
    ObjectLogger,
    LocalObjectLogger,
)
from kopf._core.engines.admission import (
    AdmissionError,
)
from kopf._core.engines.posting import (
    event,
    info,
    warn,
    exception,
)
from kopf._core.intents.callbacks import (
    not_,
    all_,
    any_,
    none_,
)
from kopf._core.intents.causes import (
    Reason,
)
from kopf._core.intents.filters import (
    ABSENT,
    PRESENT,
)
from kopf._core.intents.registries import (
    OperatorRegistry,
    get_default_registry,
    set_default_registry,
)
from kopf._core.intents.stoppers import (
    DaemonStopped,
    DaemonStoppingReason,
    SyncDaemonStopperChecker,  # deprecated
    AsyncDaemonStopperChecker,  # deprecated
)
from kopf._core.intents.piggybacking import (
    login_via_pykube,
    login_via_client,
    login_with_kubeconfig,
    login_with_service_account,
)
from kopf._core.reactor.running import (
    spawn_tasks,
    run_tasks,
    operator,
    run,
)
from kopf._core.reactor.subhandling import (
    execute,
)
from kopf._kits.hierarchies import (
    adopt,
    label,
    harmonize_naming,
    adjust_namespace,
    append_owner_reference,
    remove_owner_reference,
)
from kopf._kits.webhooks import (
    WebhookServer,
    WebhookK3dServer,
    WebhookMinikubeServer,
    WebhookNgrokTunnel,
    WebhookAutoServer,
    WebhookAutoTunnel,
)

__all__ = [
    'on', 'lifecycles', 'register', 'execute', 'daemon', 'timer', 'index',
    'configure', 'LogFormat',
    'login_via_pykube',
    'login_via_client',
    'login_with_kubeconfig',
    'login_with_service_account',
    'LoginError',
    'ConnectionInfo',
    'event', 'info', 'warn', 'exception',
    'spawn_tasks', 'run_tasks', 'operator', 'run',
    'adopt', 'label',
    'not_',
    'all_',
    'any_',
    'none_',
    'get_default_lifecycle', 'set_default_lifecycle',
    'build_object_reference', 'build_owner_reference',
    'append_owner_reference', 'remove_owner_reference',
    'ErrorsMode',
    'AdmissionError',
    'WebhookClientConfigService',
    'WebhookClientConfig',
    'Operation',
    'UserInfo',
    'Headers',
    'SSLPeer',
    'WebhookFn',
    'WebhookServerProtocol',
    'WebhookServer',
    'WebhookK3dServer',
    'WebhookMinikubeServer',
    'WebhookNgrokTunnel',
    'WebhookAutoServer',
    'WebhookAutoTunnel',
    'PermanentError',
    'TemporaryError',
    'HandlerTimeoutError',
    'HandlerRetriesError',
    'OperatorRegistry',
    'get_default_registry',
    'set_default_registry',
    'PRESENT', 'ABSENT',
    'OperatorSettings',
    'DiffBaseStorage',
    'AnnotationsDiffBaseStorage',
    'StatusDiffBaseStorage',
    'MultiDiffBaseStorage',
    'ProgressRecord',
    'ProgressStorage',
    'AnnotationsProgressStorage',
    'StatusProgressStorage',
    'MultiProgressStorage',
    'SmartProgressStorage',
    'RawEventType',
    'RawEvent',
    'RawBody',
    'Status',
    'Spec',
    'Meta',
    'Body',
    'BodyEssence',
    'Labels',
    'Annotations',
    'ObjectReference',
    'OwnerReference',
    'Memo', 'Index', 'Store',
    'Logger',
    'ObjectLogger',
    'LocalObjectLogger',
    'FieldSpec',
    'FieldPath',
    'Diff',
    'DiffItem',
    'DiffOperation',
    'HandlerId',
    'Reason',
    'Patch',
    'DaemonStopped',
    'DaemonStoppingReason',
    'SyncDaemonStopperChecker',  # deprecated
    'AsyncDaemonStopperChecker',  # deprecated
    'Resource', 'EVERYTHING',
]
