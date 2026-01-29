"""
A connection between object logger and background k8s-event poster.

Everything logged to the object logger (except for debug information) is also
posted as a k8s-event -- in the background by :mod:`kopf._core.engines.posting`.

This eliminates the need to log & post the same messages, which complicates
the operators' code, and can lead to information loss or mismatch
(e.g. when logging call is added, but posting is forgotten).
"""
import copy
import enum
import logging
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, TextIO

# Luckily, we do not mock these ones in tests, so we can import them into our namespace.
try:
    # python-json-logger>=3.1.0
    from pythonjsonlogger.core import RESERVED_ATTRS as _pjl_RESERVED_ATTRS
    from pythonjsonlogger.json import JsonFormatter as _pjl_JsonFormatter
except ImportError:
    # python-json-logger<3.1.0
    from pythonjsonlogger.jsonlogger import JsonFormatter as _pjl_JsonFormatter  # type: ignore
    from pythonjsonlogger.jsonlogger import RESERVED_ATTRS as _pjl_RESERVED_ATTRS  # type: ignore

from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies

logger = logging.getLogger('kopf.objects')

# A key for object references in JSON logs, as seen by the log parsers.
DEFAULT_JSON_REFKEY = 'object'


class LogFormat(enum.Enum):
    """ Log formats, as specified on CLI. """
    PLAIN = '%(message)s'
    FULL = '[%(asctime)s] %(name)-20.20s [%(levelname)-8.8s] %(message)s'
    JSON = '-json-'  # not used for formatting, only for detection


class ObjectFormatter(logging.Formatter):
    pass


class ObjectTextFormatter(ObjectFormatter, logging.Formatter):
    pass


class ObjectJsonFormatter(ObjectFormatter, _pjl_JsonFormatter):
    def __init__(
            self,
            *args: Any,
            refkey: str | None = None,
            **kwargs: Any,
    ) -> None:
        # Avoid type checking, as the args are not in the parent consructor.
        reserved_attrs = kwargs.pop('reserved_attrs', _pjl_RESERVED_ATTRS)
        reserved_attrs = set(reserved_attrs)
        reserved_attrs |= {'k8s_skip', 'k8s_ref', 'settings'}
        kwargs |= dict(reserved_attrs=reserved_attrs)
        kwargs.setdefault('timestamp', True)
        super().__init__(*args, **kwargs)
        self._refkey: str = refkey or DEFAULT_JSON_REFKEY

    def add_fields(
            self,
            log_record: dict[str, object],
            record: logging.LogRecord,
            message_dict: dict[str, object],
    ) -> None:
        super().add_fields(log_record, record, message_dict)

        if self._refkey and hasattr(record, 'k8s_ref'):
            ref = getattr(record, 'k8s_ref')
            log_record[self._refkey] = ref

        if 'severity' not in log_record:
            log_record['severity'] = (
                "debug" if record.levelno <= logging.DEBUG else
                "info" if record.levelno <= logging.INFO else
                "warn" if record.levelno <= logging.WARNING else
                "error" if record.levelno <= logging.ERROR else
                "fatal")


class ObjectPrefixingMixin(ObjectFormatter):
    def format(self, record: logging.LogRecord) -> str:
        if hasattr(record, 'k8s_ref'):
            ref = getattr(record, 'k8s_ref')
            namespace = ref.get('namespace', '')
            name = ref.get('name', '')
            prefix = f"[{namespace}/{name}]" if namespace else f"[{name}]"
            record = copy.copy(record)  # shallow
            record.msg = f"{prefix} {record.msg}"
        return super().format(record)


class ObjectPrefixingTextFormatter(ObjectPrefixingMixin, ObjectTextFormatter):
    pass


class ObjectPrefixingJsonFormatter(ObjectPrefixingMixin, ObjectJsonFormatter):
    pass


class ObjectLogger(typedefs.LoggerAdapter):
    """
    A logger/adapter to carry the object identifiers for formatting.

    The identifiers are then used both for formatting the per-object messages
    in :class:`ObjectPrefixingFormatter`, and when posting the k8s-events.

    Constructed in event handling of each individual object.

    The internal structure is made the same as an object reference in K8s API,
    but can change over time to anything needed for our internal purposes.
    However, as little information should be carried as possible,
    and the information should be protected against the object modification
    (e.g. in case of background posting via the queue; see :class:`K8sPoster`).
    """

    def __init__(self, *, body: bodies.Body, settings: configuration.OperatorSettings) -> None:
        super().__init__(logger, dict(
            settings=settings,
            k8s_skip=False,
            k8s_ref=dict(
                apiVersion=body.get('apiVersion'),
                kind=body.get('kind'),
                name=body.get('metadata', {}).get('name'),
                uid=body.get('metadata', {}).get('uid'),
                namespace=body.get('metadata', {}).get('namespace'),
            ),
        ))

    def process(
            self,
            msg: str,
            kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        # Native logging overwrites the message's extra with the adapter's extra.
        # We merge them, so that both message's & adapter's extras are available.
        kwargs["extra"] = (self.extra or {}) | kwargs.get('extra', {})
        return msg, kwargs


class LocalObjectLogger(ObjectLogger):
    """
    The same as :class:`ObjectLogger`, but does not post the messages as k8s-events.

    Used in the resource-watching handlers to log the handler's invocation
    successes/failures without overloading K8s with excessively many k8s-events.

    This class is used internally only and is not exposed publicly in any way.
    """

    def log(self, *args: Any, **kwargs: Any) -> None:
        kwargs['extra'] = dict(kwargs.pop('extra', {}), k8s_skip=True)
        return super().log(*args, **kwargs)


class TerseObjectLogger(LocalObjectLogger):
    """
    The same as 'LocalObjectLogger`, but more terse (less wordy).

    In the normal mode, only logs warnings & errors (but not infos).
    In the verbose mode, only logs warnings & errors & infos (but not debugs).

    Used for resource indexers: there can be hundreds or thousands of them,
    they are typically verbose, they are called often due to cluster changes
    (e.g. for pods). On the other hand, they are lightweight, so there is
    no much need to know what is happening until warnings/errors happen.
    """
    def isEnabledFor(self, level: int) -> bool:
        return super().isEnabledFor(level if level >= logging.WARNING else level - 10)


# Used to identify and remove our own handlers on re-runs in e2e tests. Every e2e test injects
# its own handler, but the previous handlers of preceding tests can have the stream closed,
# since they stream into an stderr interceptor of Click's runner, not to the real stderr.
# We have to remove the closed streams either when the test finishes, or when the new one starts.
if TYPE_CHECKING:
    class _KopfStreamHandler(logging.StreamHandler[TextIO]):
        pass
else:
    class _KopfStreamHandler(logging.StreamHandler):
        pass


def configure(
        debug: bool | None = None,
        verbose: bool | None = None,
        quiet: bool | None = None,
        log_format: LogFormat = LogFormat.FULL,
        log_prefix: bool | None = False,
        log_refkey: str | None = None,
) -> None:
    log_level = 'DEBUG' if debug or verbose else 'WARNING' if quiet else 'INFO'
    formatter = make_formatter(log_format=log_format, log_prefix=log_prefix, log_refkey=log_refkey)
    handler = _KopfStreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.handlers[:] = [h for h in logger.handlers if not isinstance(h, _KopfStreamHandler)]
    logger.addHandler(handler)
    logger.setLevel(log_level)

    # Prevent the low-level logging unless in the debug mode. Keep only the operator's messages.
    # For no-propagation loggers, add a dummy null handler to prevent printing the messages.
    for name in ['asyncio']:
        logger = logging.getLogger(name)
        logger.propagate = bool(debug)
        if not debug:
            logger.handlers[:] = [logging.NullHandler()]


def make_formatter(
        log_format: LogFormat = LogFormat.FULL,
        log_prefix: bool | None = False,
        log_refkey: str | None = None,
) -> ObjectFormatter:
    log_prefix = log_prefix if log_prefix is not None else bool(log_format is not LogFormat.JSON)
    match log_format:
        case LogFormat.JSON:
            if log_prefix:
                return ObjectPrefixingJsonFormatter(refkey=log_refkey)
            else:
                return ObjectJsonFormatter(refkey=log_refkey)
        case LogFormat():
            if log_prefix:
                return ObjectPrefixingTextFormatter(log_format.value)
            else:
                return ObjectTextFormatter(log_format.value)
        case str():
            if log_prefix:
                return ObjectPrefixingTextFormatter(log_format)
            else:
                return ObjectTextFormatter(log_format)
        case _:
            raise ValueError(f"Unsupported log format: {log_format!r}")
