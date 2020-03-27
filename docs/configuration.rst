=============
Configuration
=============

It is possible to fine-tune some aspects of Kopf-based operators,
like timeouts, synchronous handler pool sizes, automatic Kubernetes Event
creation from object-related log messages, etc.


Startup configuration
=====================

Every operator has its settings (even if there is more than one operator
in the same processes, e.g. due to :doc:`embedding`). The settings affect
how the framework behaves in details.

The settings can be modified in the startup handlers (see :doc:`startup`):

.. code-block:: python

    import kopf
    import logging

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.level = logging.WARNING
        settings.watching.session_timeout = 1 * 60
        settings.watching.stream_timeout = 10 * 60

All the settings have reasonable defaults, so the configuration should be used
only for fine-tuning when and if necessary.

For more settings, see `kopf.OperatorSettings` and :kwarg:`settings` kwarg.


Logging events
==============

``settings.posting`` allows to control which log messages should be post as
Kubernetes events. Use ``logging`` constants or integer values to set the level:
e.g., ``logging.WARNING``, ``logging.ERROR``, etc.
The default is ``logging`.INFO``.

.. code-block:: python

    import logging
    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.level = logging.ERROR

The event-posting can be disabled completely (the default is to be enabled):

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.posting.enabled = False

.. note::

    These settings also affect `kopf.event` and related functions:
    `kopf.info`, `kopf.warn`, `kopf.exception`, etc --
    even if they are called explicitly in the code.

    To avoid these settings having impact on your code, post events
    directly with an API client library instead of Kopf-provided toolkit.


Synchronous handlers
====================

``settings.execution`` allows to set the number of synchronous workers used
by the operator for synchronous handlers, or replace the asyncio executor
with another one:

.. code-block:: python

    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.execution.max_workers = 20


It is possible to replace the whole asyncio executor used
for synchronous handlers (see :doc:`async`).

Please note that the handlers that started in a previous executor, will be
continued and finished with their original executor. This includes the startup
handler itself. To avoid it, make the on-startup handler asynchronous:

.. code-block:: python

    import concurrent.futures
    import kopf

    @kopf.on.startup()
    async def configure(settings: kopf.OperatorSettings, **_):
        settings.execution.executor = concurrent.futures.ThreadPoolExecutor()


API timeouts
============

Few timeouts can be controlled when communicating with Kubernetes API:

``settings.watching.session_timeout`` (seconds) is how long the session
with a watching request will exist before terminating from the **client** side.
The default is forever (``None``).

``settings.watching.stream_timeout`` (seconds) is how long the session
with a watching request will exist before terminating from the **server** side.
The default is to let the server decide (``None``).

It makes no sense to set the client-side timeout shorter than the server side
timeout, but it is given to the developers' responsibility to decide.

The server-side timeouts are unpredictable, they can be in 10 seconds or
in 10 minutes. Yet, it feels wrong to assume any "good" values in a framework
(especially since it works without timeouts defined, just produces extra logs).

``settings.watching.retry_delay`` (seconds) is for how long to sleep between
watching requests -- in order to prevent API flooding in case of errors
or disconnects. The default is 0.1 seconds (nearly instant, but no flooding).

.. code-block:: python

    import concurrent.futures
    import kopf

    @kopf.on.startup()
    def configure(settings: kopf.OperatorSettings, **_):
        settings.watching.stream_timeout = 10 * 60
