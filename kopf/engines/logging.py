"""
A connection between object logger and background k8s-event poster.

Everything logged to the object logger (except for debug information)
is also posted as the k8s-event -- in the background: `kopf.engines.posting`.

This eliminates the need to log & post the same messages, which complicates
the operators' code, and can lead to information loss or mismatch
(e.g. when logging call is added, but posting is forgotten).
"""
import copy
import logging

from kopf.engines import posting


class ObjectPrefixingFormatter(logging.Formatter):
    """ An utility to prefix the per-object log messages. """

    def format(self, record):
        if hasattr(record, 'k8s_ref'):
            ref = record.k8s_ref
            prefix = f"[{ref['namespace']}/{ref['name']}]"
            record = copy.copy(record)  # shallow
            record.msg = f"{prefix} {record.msg}"
        return super().format(record)


class K8sPoster(logging.Handler):
    """
    A handler to post all log messages as K8s events.
    """

    def createLock(self):
        # Save some time on unneeded locks. Events are posted in the background.
        # We only put events to the queue, which is already lock-protected.
        self.lock = None

    def filter(self, record):
        # Only those which have a k8s object referred (see: `ObjectLogger`).
        # Otherwise, we have nothing to post, and nothing to do.
        has_ref = hasattr(record, 'k8s_ref')
        skipped = hasattr(record, 'k8s_skip') and record.k8s_skip
        return has_ref and not skipped and super().filter(record)

    def emit(self, record):
        # Same try-except as in e.g. `logging.StreamHandler`.
        try:
            type = (
                "Debug" if record.levelno <= logging.DEBUG else
                "Normal" if record.levelno <= logging.INFO else
                "Warning" if record.levelno <= logging.WARNING else
                "Error" if record.levelno <= logging.ERROR else
                "Fatal" if record.levelno <= logging.FATAL else
                logging.getLevelName(record.levelno).capitalize())
            reason = 'Logging'
            message = self.format(record)
            posting.enqueue(
                ref=record.k8s_ref,
                type=type,
                reason=reason,
                message=message)
        except Exception:
            self.handleError(record)


class ObjectLogger(logging.LoggerAdapter):
    """
    A logger/adapter to carry the object identifiers for formatting.

    The identifiers are then used both for formatting the per-object messages
    in `ObjectPrefixingFormatter`, and when posting the per-object k8s-events.

    Constructed in event handling of each individual object.

    The internal structure is made the same as an object reference in K8s API,
    but can change over time to anything needed for our internal purposes.
    However, as little information should be carried as possible,
    and the information should be protected against the object modification
    (e.g. in case of background posting via the queue; see `K8sPoster`).
    """

    def __init__(self, *, body):
        super().__init__(logger, dict(
            k8s_skip=False,
            k8s_ref=dict(
                apiVersion=body.get('apiVersion'),
                kind=body.get('kind'),
                name=body.get('metadata', {}).get('name'),
                uid=body.get('metadata', {}).get('uid'),
                namespace=body.get('metadata', {}).get('namespace'),
            ),
        ))

    def process(self, msg, kwargs):
        # Native logging overwrites the message's extra with the adapter's extra.
        # We merge them, so that both message's & adapter's extras are available.
        kwargs["extra"] = dict(self.extra, **kwargs.get('extra', {}))
        return msg, kwargs

    def log(self, level, msg, *args, local=False, **kwargs):
        if local:
            kwargs['extra'] = dict(kwargs.pop('extra', {}), k8s_skip=True)
        super().log(level, msg, *args, **kwargs)


logger = logging.getLogger('kopf.objects')
logger.addHandler(K8sPoster(level=logging.INFO))
