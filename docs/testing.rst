================
Operator testing
================

Kopf provides some tools to test the Kopf-based operators
via :mod:`kopf.testing` module (requires explicit importing).


Background runner
=================

:class:`kopf.testing.KopfRunner` runs an arbitrary operator in the background,
while the original testing thread does the object manipulation and assertions:

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
    The operator runs against the cluster which is currently authenticated ---
    same as if would be executed with ``kopf run``.


Mock server
===========

KMock is a supplimentary project to run a local mock server for any HTTP API, and for Kubernetes API in particular — with extended supported of Kubernetes API endpoints, resource discovery, and implicit in-memory object persistence.

Use KMock when you need to run a very lightweight simulation of the Kubernetes API without deploying the heavy Kubernetes cluster nearby, for example when migrating to/from Kopf.

.. code-block:: python

    import kmock
    import requests

    def test_object_patching(kmock: kmock.KubernetesEmulator) -> None:
        kmock.objects['kopf.dev/v1/kopfexamples', 'ns1', 'name1'] = {'spec': 123}
        requests.patch(str(kmock.url) + '/kopf.dev/v1/namespaces/ns1/name1', json={'spec': 456})
        assert len(kmock.requests) == 1
        assert kmock.requests[0].method == 'patch'
        assert kmock.objects['kopf.dev/v1/kopfexamples', 'ns1', 'name1'] == {'spec': 456}

KMock's detailed documentation is out of scope of Kopf's documentation. The project and its documentation can be found at:

* https://kmock.readthedocs.io/
* https://github.com/nolar/kmock
* https://pypi.org/project/kmock/
