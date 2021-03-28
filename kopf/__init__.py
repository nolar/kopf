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
from kopf.engines.loggers import (
    configure,
    LogFormat,
    ObjectLogger,
    LocalObjectLogger,
)
from kopf.engines.posting import (
    event,
    info,
    warn,
    exception,
)
from kopf.on import (
    subhandler,
    register,
    daemon,
    timer,
    index,
)
from kopf.reactor import (
    lifecycles,  # as a separate name on the public namespace
)
from kopf.reactor.admission import (
    AdmissionError,
)
from kopf.reactor.handling import (
    TemporaryError,
    PermanentError,
    HandlerTimeoutError,
    HandlerRetriesError,
    execute,
)
from kopf.reactor.lifecycles import (
    get_default_lifecycle,
    set_default_lifecycle,
)
from kopf.reactor.registries import (
    OperatorRegistry,
    get_default_registry,
    set_default_registry,
)
from kopf.reactor.running import (
    spawn_tasks,
    run_tasks,
    operator,
    run,
)
from kopf.storage.diffbase import (
    DiffBaseStorage,
    AnnotationsDiffBaseStorage,
    StatusDiffBaseStorage,
    MultiDiffBaseStorage,
)
from kopf.storage.progress import (
    ProgressRecord,
    ProgressStorage,
    AnnotationsProgressStorage,
    StatusProgressStorage,
    MultiProgressStorage,
    SmartProgressStorage,
)
from kopf.structs.bodies import (
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
from kopf.structs.callbacks import (
    not_,
    all_,
    any_,
    none_,
)
from kopf.structs.configuration import (
    OperatorSettings,
)
from kopf.structs.credentials import (
    LoginError,
    ConnectionInfo,
)
from kopf.structs.dicts import (
    FieldSpec,
    FieldPath,
)
from kopf.structs.diffs import (
    Diff,
    DiffItem,
    DiffOperation,
)
from kopf.structs.ephemera import (
    Memo,
    Index,
    Store,
)
from kopf.structs.filters import (
    ABSENT,
    PRESENT,
)
from kopf.structs.handlers import (
    ErrorsMode,
    Reason,
)
from kopf.structs.ids import (
    HandlerId,
)
from kopf.structs.patches import (
    Patch,
)
from kopf.structs.primitives import (
    DaemonStoppingReason,
    SyncDaemonStopperChecker,
    AsyncDaemonStopperChecker,
)
from kopf.structs.references import (
    Resource,
    EVERYTHING,
)
from kopf.structs.reviews import (
    WebhookClientConfigService,
    WebhookClientConfig,
    Operation,
    UserInfo,
    Headers,
    SSLPeer,
    WebhookFn,
    WebhookServerProtocol,
)
from kopf.toolkits.hierarchies import (
    adopt,
    label,
    harmonize_naming,
    adjust_namespace,
    append_owner_reference,
    remove_owner_reference,
)
from kopf.toolkits.webhooks import (
    WebhookServer,
    WebhookK3dServer,
    WebhookMinikubeServer,
    WebhookNgrokTunnel,
    WebhookAutoServer,
    WebhookAutoTunnel,
)
from kopf.utilities.piggybacking import (
    login_via_pykube,
    login_via_client,
)

__all__ = [
    'on', 'lifecycles', 'register', 'execute', 'daemon', 'timer', 'index',
    'configure', 'LogFormat',
    'login_via_pykube', 'login_via_client', 'LoginError', 'ConnectionInfo',
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
    'DaemonStoppingReason',
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
    'SyncDaemonStopperChecker',
    'AsyncDaemonStopperChecker',
    'Resource', 'EVERYTHING',
]
