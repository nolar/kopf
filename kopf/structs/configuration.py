"""
All configuration flags, options, settings to fine-tune an operator.

All settings are grouped semantically just for convenience
(instead of a flat mega-object with all the values in it).

The individual groups or settings can eventually be moved or regrouped within
the root object, while keeping the legacy names for backward compatibility.

.. note::

    There is a discussion on usage of such words as "configuration",
    "preferences", "settings", "options", "properties" in the internet:

    * https://stackoverflow.com/q/2074384/857383
    * https://qr.ae/pNvt40
    * etc.

    In this framework, they are called *"settings"* (plural).
    Combined, they form a *"configuration"* (singular).

    Some of the settings are flags, some are scalars, some are optional,
    some are not (but all of them have reasonable defaults).

    Regardless of the exact class and module names, all of these terms can be
    used interchangeably -- but so that it is understandable what is meant.
"""
import dataclasses
from typing import Optional

from kopf import config  # for legacy defaults only


@dataclasses.dataclass
class LoggingSettings:
    pass


@dataclasses.dataclass
class PostingSettings:

    level: int = dataclasses.field(
        default_factory=lambda: config.EventsConfig.events_loglevel)
    """
    A minimal level of logging events that will be posted as K8s Events.
    The default is ``logging.INFO`` (i.e. all info, warning, errors are posted).

    This also affects ``kopf.event()`` and similar functions
    (``kopf.info()``, ``kopf.warn()``, ``kopf.exception()``).
    """


@dataclasses.dataclass
class WatchingSettings:

    session_timeout: Optional[float] = dataclasses.field(
        default_factory=lambda: config.WatchersConfig.session_timeout)
    """
    An HTTP/HTTPS session timeout to use in watch requests.
    """

    stream_timeout: Optional[float] = dataclasses.field(
        default_factory=lambda: config.WatchersConfig.default_stream_timeout)
    """
    The maximum duration of one streaming request. Patched in some tests.
    If ``None``, then obey the server-side timeouts (they seem to be random).
    """

    retry_delay: float = dataclasses.field(
        default_factory=lambda: config.WatchersConfig.watcher_retry_delay)
    """
    How long should a pause be between watch requests (to prevent API flooding).
    """


@dataclasses.dataclass
class BatchingSettings:
    """
    Settings for how raw events are batched and processed.
    """

    worker_limit: Optional[int] = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.queue_workers_limit)
    """
    How many workers can be running simultaneously on per-object event queue.
    If ``None``, there is no limit to the number of workers (as many as needed).
    """

    idle_timeout: float = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.worker_idle_timeout)
    """
    How soon an idle worker is exited and garbage-collected if no events arrive.
    """

    batch_window: float = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.worker_batch_window)
    """
    How fast/slow does a worker deplete the queue when an event is received.
    All events arriving within this window will be ignored except the last one.
    """

    exit_timeout: float = dataclasses.field(
        default_factory=lambda: config.WorkersConfig.worker_exit_timeout)
    """
    How soon a worker is cancelled when the parent watcher is going to exit.
    This is the time given to the worker to deplete and process the queue.
    """



@dataclasses.dataclass
class OperatorSettings:
    logging: LoggingSettings = dataclasses.field(default_factory=LoggingSettings)
    posting: PostingSettings = dataclasses.field(default_factory=PostingSettings)
    watching: WatchingSettings = dataclasses.field(default_factory=WatchingSettings)
    batching: BatchingSettings = dataclasses.field(default_factory=BatchingSettings)
