=========
Embedding
=========

Kopf is designed to be embeddable into other applications, which require
watching over the Kubernetes resources (custom or built-in), and handling
the changes.
This can be used, for example, in desktop applications or web APIs/UIs
to keep the state of the cluster and its resources in memory.


Manual execution
================

Since Kopf is fully asynchronous, the best way to run Kopf is to provide
an event-loop in a separate thread, which is dedicated to Kopf,
while running the main application in the main thread:

.. code-block:: python

    import asyncio
    import threading

    import kopf

    @kopf.on.create('kopfexamples')
    def create_fn(**_):
        pass

    def kopf_thread():
        asyncio.run(kopf.operator())

    def main():
        thread = threading.Thread(target=kopf_thread)
        thread.start()
        # ...
        thread.join()

In the case of :command:`kopf run`, the main application is Kopf itself,
so its event-loop runs in the main thread.

.. note::
    When an asyncio task runs not in the main thread, it cannot set
    the OS signal handlers, so a developer should implement the termination
    themselves (cancellation of an operator task is enough).


Manual orchestration
====================

Alternatively, a developer can orchestrate the operator's tasks and sub-tasks
themselves. The example above is an equivalent of the following:

.. code-block:: python

    def kopf_thread():
        loop = asyncio.get_event_loop_policy().get_event_loop()
        tasks = loop.run_until_complete(kopf.spawn_tasks())
        loop.run_until_complete(kopf.run_tasks(tasks, return_when=asyncio.FIRST_COMPLETED))

Or, if proper cancellation and termination are not expected, of the following:

.. code-block:: python

    def kopf_thread():
        loop = asyncio.get_event_loop_policy().get_event_loop()
        tasks = loop.run_until_complete(kopf.spawn_tasks())
        loop.run_until_complete(asyncio.wait(tasks))

In all cases, make sure that asyncio event loops are properly used.
Specifically, :func:`asyncio.run` creates and finalises a new event loop
for a single call. Several calls cannot share the coroutines and tasks.
To make several calls, either create a new event loop, or get the event loop
of the current asyncio _context_ (by default, of the current thread).
See more on the asyncio event loops and _contexts_ in `Asyncio Policies`__.

__ https://docs.python.org/3/library/asyncio-policy.html

.. _custom-event-loops:


Custom event loops
==================

Kopf can run in any AsyncIO-compatible event loop. For example, uvloop `claims to be 2x–2.5x times faster`__ than asyncio. To run Kopf in uvloop, call it this way:

__ http://magic.io/blog/uvloop-blazing-fast-python-networking/

.. code-block:: python

    import kopf
    import uvloop

    def main():
        loop = uvloop.EventLoopPolicy().get_event_loop()
        loop.run(kopf.operator())

Or this way:

.. code-block:: python

    import kopf
    import uvloop

    def main():
        kopf.run(loop=uvloop.EventLoopPolicy().new_event_loop())

Or this way:

.. code-block:: python

    import kopf
    import uvloop

    def main():
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        kopf.run()

Or any other way the event loop prescribes in its documentation.

Kopf's CLI (i.e. :command:`kopf run`) will use uvloop by default if it is installed. To disable this implicit behaviour, either uninstall uvloop from Kopf's environment, or run Kopf explicitly from the code using the standard event loop.

For convenience, Kopf can be installed as ``pip install kopf[uvloop]`` to enable this mode automatically.

Kopf will never implicitly activate the custom event loops if it is called from the code, not from the CLI.


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
        asyncio.run(kopf.operator(
            registry=registry,
        ))

    def main():
        thread = threading.Thread(target=kopf_thread)
        thread.start()
        # ...
        thread.join()


.. warning::
    It is not recommended to run Kopf in the same event-loop as other routines
    or applications: it considers all tasks in the event-loop as spawned by its
    workers and handlers, and cancels them when it exits.

    There are some basic safety measures to not cancel tasks existing prior
    to the operator's startup, but that cannot be applied to the tasks spawned
    later due to asyncio implementation details.
