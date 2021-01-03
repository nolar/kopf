=========
Embedding
=========

Kopf is designed to be embeddable into other applications, which require
watching over the Kubernetes resources (custom or built-in), and handling
the changes.
This can be used, for example, in desktop applications or web APIs/UIs
to keep the state of the cluster and its resources in memory.


Manual orchestration
====================

Since Kopf is fully asynchronous, the best way to run Kopf is to provide
an event-loop specially for Kopf in a separate thread, while running
the main application in the main thread.

.. code-block:: python

    import asyncio
    import threading

    import kopf

    @kopf.on.create('kopfexamples')
    def create_fn(**_):
        pass

    def kopf_thread():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(kopf.operator())

    def main():
        thread = threading.Thread(target=kopf_thread)
        thread.start()
        # ...
        thread.join()

In case of :command:`kopf run`, the main application is Kopf itself,
so its event-loop runs in the main thread.

.. note::
    When an asyncio task runs not in the main thread, it cannot set
    the OS signal handlers, so a developer should implement the termination
    themselves (cancellation of an operator task is enough).

Alternatively, a developer can orchestrate the operator's tasks and sub-tasks
themselves. The example above is an equivalent of the following:

.. code-block:: python

    def kopf_thread():
        loop = asyncio.get_event_loop()
        tasks = loop.run_until_complete(kopf.spawn_tasks())
        loop.run_until_complete(kopf.run_tasks(tasks, return_when=asyncio.FIRST_COMPLETED))

Or, if proper cancellation and termination is not expected, of the following:

.. code-block:: python

    def kopf_thread():
        loop = asyncio.get_event_loop()
        tasks = loop.run_until_complete(kopf.spawn_tasks())
        loop.run_until_complete(asyncio.wait(tasks))


Multiple operators
==================

Kopf can handle multiple resources at a time, so only one instance should be
sufficient for most cases. However, it can be needed to run multiple isolated
operators in the same process.

It should be safe to run multiple operators in multiple isolated event-loops.
Despite Kopf's routines use the global state, all such a global state is stored
in :mod:`contextvars` containers with values isolated per-loop and per-task.

.. code-block:: python

    import asyncio
    import threading

    import kopf

    registry = kopf.OperatorRegistry()

    @kopf.on.create('kopfexamples', registry=registry)
    def create_fn(**_):
        pass

    def kopf_thread():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(kopf.operator(
            registry=registry,
        ))

    def main():
        thread = threading.Thread(target=kopf_thread)
        thread.start()
        # ...
        thread.join()


.. warning::
    It is not recommended to run Kopf in the same event-loop with other routines
    or applications: it considers all tasks in the event-loop as spawned by its
    workers and handlers, and cancels them when it exits.

    There are some basic safety measures to not cancel tasks existing prior
    to the operator's startup, but that cannot be applied to the tasks spawned
    later due to asyncio implementation details.
