================
Operator testing
================

Kopf provides some tools for testing Kopf-based operators
via the :mod:`kopf.testing` module (requires explicit importing).


Programmatic runners
====================

:class:`kopf.testing.KopfRunner` enters the operator through the CLI,
which requires module paths and Click invocation.
For cases where the handlers are already registered in the process
(e.g. imported directly in the test module),
there are two programmatic runners that enter via :func:`kopf.operator` directly.

Unlike the CLI runner, programmatic runners do not import any files or modules.
Instead, they inherit the caller's environment (i.e., the handlers),
unless a custom registry is passed as an argument.


Background thread
-----------------

:class:`kopf.testing.KopfThread` is a sync context manager
that runs the operator in a background thread:

.. code-block:: python

    import kopf
    import time
    from kopf.testing import KopfThread

    @kopf.on.create('kopfexamples')
    def create_fn(**_):
        pass

    def test_operator_in_a_thread():
        settings = kopf.OperatorSettings()
        settings.scanning.disabled = True
        with KopfThread(namespaces=['ns1'], settings=settings):
            # do something while the operator is running.
            time.sleep(3)
        # operator has been stopped and cleaned up


Background task
---------------

:class:`kopf.testing.KopfTask` is an async context manager
that runs the operator as a background asyncio task:

.. code-block:: python

    import kopf
    from kopf.testing import KopfTask

    @kopf.on.create('kopfexamples')
    def create_fn(**_):
        pass

    async def test_operator_in_a_task():
        settings = kopf.OperatorSettings()
        settings.scanning.disabled = True
        async with KopfTask(namespaces=['ns1'], settings=settings):
            # do something while the operator is running.
            pass
        # operator has been stopped and cleaned up


Common options
--------------

Both :class:`kopf.testing.KopfThread` and :class:`kopf.testing.KopfTask`
accept all the same keyword arguments as :func:`kopf.operator`,
plus two additional ones:

* :kwarg:`timeout` --- how long to wait for the operator to stop on exit
  (in seconds). If the operator does not stop in time, an exception is raised.
  ``None`` means wait indefinitely (the default).
* :kwarg:`reraise` --- whether to propagate exceptions from the operator
  (default ``True``). If the ``with`` block also raised,
  the operator exception is chained.

If :kwarg:`stop_flag` is not provided, one is injected automatically
and set when the context manager exits.
If :kwarg:`ready_flag` is provided, it is passed through to the operator
and can be awaited inside the ``with`` block.


Command-line runner
===================

:class:`kopf.testing.KopfRunner` runs an arbitrary operator in the background
while the original testing thread performs object manipulation and assertions:

When the ``with`` block exits, the operator stops, and its exceptions,
exit code and output are available to the test (for additional assertions).

.. code-block:: python
    :caption: test_example_operator.py

    import time
    import subprocess
    from kopf.testing import KopfRunner

    def test_operator():
        with KopfRunner(['run', '-A', '--verbose', 'examples/01-minimal/example.py']) as runner:
            # do something while the operator is running.

            subprocess.run("kubectl apply -f examples/obj.yaml", shell=True, check=True)
            time.sleep(1)  # give it some time to react and to sleep and to retry

            subprocess.run("kubectl delete -f examples/obj.yaml", shell=True, check=True)
            time.sleep(1)  # give it some time to react

        assert runner.exit_code == 0
        assert runner.exception is None
        assert 'And here we are!' in runner.output
        assert 'Deleted, really deleted' in runner.output

.. note::
    The operator runs against the currently authenticated cluster ---
    the same as if it were executed with ``kopf run``.


Mock server
===========

KMock is a supplementary project for running a local mock server for any HTTP API, and for the Kubernetes API in particular --- with extended support for Kubernetes API endpoints, resource discovery, and implicit in-memory object persistence.

Use KMock when you need a very lightweight simulation of the Kubernetes API without deploying a full Kubernetes cluster, for example when migrating to/from Kopf.

.. code-block:: python

    import kmock
    import requests

    def test_object_patching(kmock: kmock.KubernetesEmulator) -> None:
        kmock.objects['kopf.dev/v1/kopfexamples', 'ns1', 'name1'] = {'spec': 123}
        requests.patch(str(kmock.url) + '/kopf.dev/v1/namespaces/ns1/name1', json={'spec': 456})
        assert len(kmock.requests) == 1
        assert kmock.requests[0].method == 'patch'
        assert kmock.objects['kopf.dev/v1/kopfexamples', 'ns1', 'name1'] == {'spec': 456}

KMock's detailed documentation is outside the scope of Kopf's documentation. The project and its documentation can be found at:

* https://kmock.readthedocs.io/
* https://github.com/nolar/kmock
* https://pypi.org/project/kmock/
