================
Configuration
================

There are tools to configure some of kopf functionality, like asynchronous
tasks behaviour and logging events.

.. note::
    All configuration should be done before importing kopf modules.


Configure logging events
=================

`kopf.config.EventsConfig` allows to set what types of kopf logs should be
reflected in events.

Loglevels are:

* ``kopf.config.LOGLEVEL_INFO``
* ``kopf.config.LOGLEVEL_WARNING``
* ``kopf.config.LOGLEVEL_ERROR``
* ``kopf.config.LOGLEVEL_CRITICAL``

.. code-block:: python
    :caption: test_example_operator.py

    from kopf import config

    # Now kopf will send events only when error or critical occasion happens
    config.EventsConfig.events_loglevel = config.LOGLEVEL_ERROR

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
    def create_fn(body, **kwargs):
        print(f"A handler is called with body: {body}")


Configure Workers
=================

`kopf.config.WorkersConfig` allows to set numbers of workers, launch periods,
and timeouts for many kinds of tasks.

.. code-block:: python
    :caption: test_example_operator.py

    from kopf import config

    # Let's set how many workers can be running simultaneously on per-object event queue
    config.WorkersConfig.queue_workers_limit = 10

    import kopf

    @kopf.on.create('zalando.org', 'v1', 'ephemeralvolumeclaims')
    def create_fn(body, **kwargs):
        print(f"A handler is called with body: {body}")


