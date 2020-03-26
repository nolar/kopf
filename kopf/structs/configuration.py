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
class OperatorSettings:
    logging: LoggingSettings = dataclasses.field(default_factory=LoggingSettings)
    posting: PostingSettings = dataclasses.field(default_factory=PostingSettings)
