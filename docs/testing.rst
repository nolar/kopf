================
Operator testing
================

Kopf provides some tools to test the Kopf-based operators
via `kopf.testing` module (requires explicit importing).


Background runner
=================

`kopf.testing.KopfRunner` runs an arbitrary operator in the background,
while the original testing thread does the object manipulation and assertions:

When the ``with`` block exits, the operator stops, and its exceptions,
exit code, and output are available to the test (for additional assertions).

.. code-block:: python
    :caption: test_example_operator.py

    import shlex
    import subprocess
    from kopf.testing import KopfRunner

    def test_operator():
        with KopfRunner(['run', '--verbose', 'examples/01-minimal/example.py']) as runner:
            # do something while the operator is running.

            subprocess.run("kubectl apply -f examples/obj.yaml", shell=True, check=True)
            time.sleep(1)  # give it some time to react and to sleep and to retry

            subprocess.run("kubectl delete -f examples/obj.yaml", shell=True, check=True)
            time.sleep(1)  # give it some time to react

        assert runner.exit_code == 0
        assert runner.exception is None
        assert 'And here we are!' in runner.stdout
        assert 'Deleted, really deleted' in runner.stdout

.. note::
    The operator runs against the cluster which is currently authenticated ---
    same as if would be executed with `kopf run`.
