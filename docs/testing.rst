================
Operator testing
================

Kopf provides some tools for testing Kopf-based operators
via the :mod:`kopf.testing` module (requires explicit importing).


Background runner
=================

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


Handler isolation
-----------------

:class:`kopf.testing.KopfRunner` is isolated from the caller's environment
by default: it creates its own registry and settings, so only the handlers
from the specified files and modules are loaded --- the same as ``kopf run``
would behave on the command line.

This means that any ``@kopf.on.*`` handlers defined in the test file
or elsewhere in the caller's code are not visible to the runner
and do not affect the operator being tested.

If the caller's handlers must be passed to the operator (not recommended), pass
the caller's default registry explicitly from :func:`kopf.get_default_registry`
--- this is where the handlers go to by default:

.. code-block:: python

    import kopf
    from kopf.testing import KopfRunner

    with KopfRunner(
        ['run', '--verbose', 'example.py'],
        registry=kopf.get_default_registry(),
    ) as runner:
        ...


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
