================
Configuration
================

There are tools to configure some of kopf functionality, like asynchronous
tasks behaviour and logging events.


Configure logging events
========================

`kopf.config.EventsConfig` allows to set what types of kopf logs should be
reflected in events.

Loglevels are:

* ``kopf.config.LOGLEVEL_INFO``
* ``kopf.config.LOGLEVEL_WARNING``
* ``kopf.config.LOGLEVEL_ERROR``
* ``kopf.config.LOGLEVEL_CRITICAL``

.. code-block:: python

    import kopf

    # Now kopf will send events only when error or critical occasion happens
    kopf.EventsConfig.events_loglevel = config.LOGLEVEL_ERROR

Configure Workers
=================

`kopf.config.WorkersConfig` allows to set numbers of workers, launch periods,
and timeouts for many kinds of tasks.

.. code-block:: python

    import kopf

    # Let's set how many workers can be running simultaneously on per-object event queue
    kopf.WorkersConfig.synchronous_tasks_threadpool_limit = 20

